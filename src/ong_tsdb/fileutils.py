# -*- coding: utf-8 -*-
"""
File utilities for chunk storage: reading, writing, verifying, and
repairing binary chunk files with automatic compression detection.
"""

import sys
import time
import os
from pathlib import Path
import re
from ong_utils import is_windows

if not is_windows:
    import grp
    from pwd import getpwnam
import gzip
import stat
import numpy as np
from ong_tsdb import (
    config,
    BASE_DIR,
    COMPRESSION_EXT,
    COMPRESSION_GZIP,
    COMPRESSION_ZSTD,
    logger,
    DTYPE,
    CHUNK_ROWS,
)
from pprint import pprint

# Lazy zstandard import: only required when the user writes zstd chunks
# or migrates to them. Reading an existing gzip chunk works without it.
_zstd_available = None


def _zstd_present():
    global _zstd_available
    if _zstd_available is None:
        try:
            import zstandard  # noqa: F401

            _zstd_available = True
        except ImportError:
            _zstd_available = False
    return _zstd_available


def _zstd_compress(data: bytes, level: int = 3) -> bytes:
    import zstandard

    cctx = zstandard.ZstdCompressor(level=level)
    return cctx.compress(data)


def _zstd_decompress(data: bytes) -> bytes:
    import zstandard

    dctx = zstandard.ZstdDecompressor()
    return dctx.decompress(data)


def _zstd_decompress_stream(fileobj):
    """Wrap a zstd stream reader so the result behaves like a file
    (with ``read()`` / ``__enter__`` / ``__exit__``)."""
    import zstandard

    return zstandard.ZstdDecompressor().stream_reader(fileobj)


class _StdlibProgressBar:
    """Minimal stderr-based progress bar, used only when tqdm is not
    installed. API mirrors the subset of tqdm we need: `update(n)` and
    `close()`.
    """

    def __init__(self, total, desc="Verifying chunks"):
        self.total = total
        self.desc = desc
        self.count = 0
        self._last_print = 0
        self._step = max(total // 20, 1) if total else 1
        self._closed = False

    def update(self, n=1):
        if self._closed:
            return
        self.count += n
        if self.count - self._last_print >= self._step:
            pct = (100 * self.count / self.total) if self.total else 0
            print(
                f"\r{self.desc}: {self.count}/{self.total} ({pct:.0f}%)",
                end="",
                file=sys.stderr,
                flush=True,
            )
            self._last_print = self.count

    def close(self):
        if self._closed or not self.total:
            return
        # If update() never reached 100% (e.g. fewer than step), print it.
        if self.count != self.total:
            print(
                f"\r{self.desc}: {self.total}/{self.total} (100%)",
                end="",
                file=sys.stderr,
                flush=True,
            )
        # Trailing newline so the next stderr line is not glued to the bar.
        print(file=sys.stderr, flush=True)
        self._closed = True


def _make_progress_bar(total, desc="Verifying chunks"):
    """Return a progress bar object with update()/close() API.

    Uses tqdm if available; falls back to a stdlib-only stderr bar.
    Returns None if total is 0 (nothing to show).
    """
    if not total:
        return None
    try:
        from tqdm import tqdm

        return tqdm(
            total=total,
            desc=desc,
            unit="chunk",
            file=sys.stderr,
            mininterval=0.2,
            disable=False,
        )
    except ImportError:
        return _StdlibProgressBar(total, desc)


# Regular expression for parsing chunk filenames. Anchored at end (\Z) so
# that leftover or renamed files (e.g. "1234.5.gz.bak") do not partially
# match and silently produce wrong metadata. The compression suffix accepts
# either of the supported codecs (gzip and zstd); uncompressed chunks
# (no extension) also match.
_KNOWN_EXTS_ALT = (
    f"{re.escape(COMPRESSION_GZIP.lstrip('.'))}|"
    f"{re.escape(COMPRESSION_ZSTD.lstrip('.'))}"
)
re_chunk_filename = re.compile(
    rf"(?P<timestamp>\d+)\.(?P<n_columns>\d+)(?P<compression>\.(?:{_KNOWN_EXTS_ALT}))?\Z"
)


def extract_filename_parts(filename):
    """Returns named groups from filename using re_chunk_filename regular expression.

    Raises ValueError if the filename does not match the chunk pattern.
    """
    m = re_chunk_filename.fullmatch(os.path.basename(filename))
    if m is None:
        raise ValueError(
            f"Filename {filename!r} does not match chunk pattern "
            f"(expected '<timestamp>.<n_columns>[.gz|.zst]')"
        )
    return m.groupdict()


def generate_filename_from_parts(path, timestamp, n_columns, compression=""):
    """Creates a chunk filename from parts, where kwargs will be used to generate regular expression"""
    if not isinstance(timestamp, str):
        timestamp = f"{timestamp:.0f}"
    chunk_filename = f"{timestamp}.{n_columns}{compression}"
    return os.path.join(path, chunk_filename)


def _get_subdirs(path):
    """Returns the list of subdirs of current path"""
    return [n for n in os.listdir(path) if os.path.isdir(os.path.join(path, n))]


def _get_chunkfiles(path):
    """Returns a sorted list of chunk (either compressed or uncompressed files), excluding empty files"""
    p = Path(path)
    files = [
        f.name
        for f in p.iterdir()
        if f.is_file() and re_chunk_filename.fullmatch(f.name) and f.stat().st_size > 0
    ]
    files.sort()
    return files


def _get_chunkcolumns(filename):
    # Returns number of columns of the chunk from its filename.
    # Chunks have a "{timestamp}.{n_columns}[.gz]" name, so extracts the {size} part
    return int(extract_filename_parts(filename)["n_columns"])


class FileUtils(object):
    """
    Class to manage files and dirs with correct permissions
    """

    def __init__(
        self,
        base_path=BASE_DIR,
        file_user=config("FILE_USER", os.getuid()),
        file_group=config("FILE_GROUP", os.getgid()),
    ):
        """
        Creates FileUtils object

        Args
            base_path : string
                Base folder of the database
            file_user : string
                Name of the owner user of new files
            file_group : string
                Name of the group owner of new files
        Raises
            Exception if file_user or file_group does not exist
        """
        try:
            self.groupid = (
                file_group
                if isinstance(file_group, int)
                else grp.getgrnam(file_group).gr_gid
            )
            self.userid = (
                file_user if isinstance(file_user, int) else getpwnam(file_user).pw_uid
            )
        except (KeyError, OSError) as e:
            raise KeyError(
                "User or Group {} does not exist. Create it with the setup script install.sh".format(
                    file_group
                )
            ) from e

        self.__path = os.path.abspath(base_path or "..")
        # Public alias used by helpers like migrate_compression that
        # need to resolve bare filenames returned by getchunks().
        self.base_path = self.__path
        if not os.path.isdir(base_path):
            admin_token = config("admin_token", None)
            if admin_token is None:
                raise Exception(
                    'Database cannot be created, add "admin_token" to your configuration file'
                )
            # Create root dir
            os.makedirs(base_path)
            self.__path = os.path.abspath(base_path)
            self.base_path = self.__path
            with self.safe_createfile(self.path_config(), "w") as f:
                f.write(admin_token)

    def path(self, *args):
        """
        Returns full path joining the args to the internal path
        """
        return os.path.join(self.__path, *args)

    def path_config(self, *args):
        """
        Returns full path joining the args to the internal path
        and adding the name of the config file (CONFIG.JSON)
        """
        return os.path.join(self.path(*args), "CONFIG.JSON")

    def safe_makedirs(self, path, *args):
        """
        Calls os.makedirs with exists_ok=Ture and then fixes permissions
        """
        os.makedirs(path, *args, exist_ok=True)
        self.__fix_permissions(path)

    def getdbs(self):
        """
        Returns list of existing databases
        """
        return _get_subdirs(self.path())

    def getsensors(self, db_name):
        """
        Returns list of existing sensor of a certain database

        Args
            db_name : string
                Database name
        Return
            list of sensors
        Raises
            OSError if db does not exist
        """
        return _get_subdirs(self.path(db_name))

    def getchunks(self, db_name, sensor):
        """
        Returns list of existing sensor of a certain database

        Args
            db_name : string
                Database name
            sensor : string
                Sensor name
        Return
            list of sensors
        Raises
            OSError if db does not exist
        """
        return _get_chunkfiles(self.path(db_name, sensor))

    def __fix_permissions(self, path):
        """
        Changes permissions and owner of a file/directory
        Permissions are set to read+write for the user and group used
        to initialize the class, no permissions for others.
        Not the safer implementation (as it is changed after file creation)
        """
        stat_mode = (
            stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP
        )  # This is 0o600 in octal and 384 in decimal.
        if os.path.isdir(path):
            stat_mode = stat_mode | stat.S_IXUSR | stat.S_IXGRP
        os.chmod(path, stat_mode)
        os.chown(path, self.userid, self.groupid)

    def safe_createfile(self, path, mode="w"):
        """
        Opens `path` for writing atomically.

        Data is written to a sibling temp file first; on close, the file is
        renamed to its final name with `os.replace`, which is atomic on
        POSIX and on Windows (Python 3.3+). If the process dies mid-write,
        the original file (if any) is preserved and the `.tmp` file can be
        safely removed. A `__cleanup_stale_tmp` call at open() time
        removes any leftover temp file from a previous crashed writer.

        Returns a file-like object whose `close()` performs the rename.
        Raises OSError if `path` is not valid.
        """
        # Clean up any leftover tmp from a previous crashed writer
        for leftover in self._stale_tmp_files_for(path):
            try:
                os.remove(leftover)
            except OSError:
                pass

        tmp_path = f"{path}.tmp.{os.getpid()}.{id(self)}"
        f = self.get_open_func(tmp_path)(tmp_path, mode)
        _original_close = f.close

        def _safe_close():
            _original_close()
            try:
                os.replace(tmp_path, path)
            except FileNotFoundError:
                # Nothing was written; tmp_path may not exist if mode was "r"
                pass

        f.close = _safe_close
        self.__fix_permissions(tmp_path)
        return f

    def _stale_tmp_files_for(self, path):
        """Lists the names of stale .tmp.* files that would correspond to
        a write of `path` (i.e. a previous writer crashed before close).
        """
        parent = os.path.dirname(path) or "."
        prefix = os.path.basename(path) + ".tmp."
        try:
            entries = os.listdir(parent)
        except OSError:
            return []
        return [
            os.path.join(parent, e)
            for e in entries
            if e.startswith(prefix) and os.path.isfile(os.path.join(parent, e))
        ]

    def __verify_chunk_content(self, filename, dtype=DTYPE, print_summary_stats=True):
        """Prints to screen the analysis of the chunk file filename"""
        arr = self.fast_read_np(filename, dtype=dtype)
        if arr.shape[0] != CHUNK_ROWS:
            logger.error(
                f"Error in {filename}: expected {CHUNK_ROWS} rows but file has {arr.shape[0]}"
            )
        index = arr[:, 0].nonzero()[0]
        min_index = index[0] if len(index) > 0 else -1
        max_index = index[-1] if len(index) > 0 else -1
        stat = dict(
            filename=filename,
            rows_total=len(arr),
            rows_used=len(index),
            rows_used_ratio_pct=len(index) / float(len(arr)) * 100,
            row_index_min=min_index,
            row_index_max=max_index,
            ratio_max_index=(max_index + 1) / float(len(arr)),
        )
        if print_summary_stats:
            pprint(stat)
        return stat

    def _iter_chunks(self, filter_db_name=None):
        """Generator that yields (db_name, sensor, sensorpath, cf, ts, diff, date)
        for every chunk in the database, one at a time, without building any
        materialised list in memory."""
        for db_name in self.getdbs():
            if filter_db_name and db_name != filter_db_name:
                continue
            for sensor in self.getsensors(db_name):
                sensorpath = self.path(db_name, sensor)
                chunkfiles = _get_chunkfiles(sensorpath)
                if not chunkfiles:
                    continue
                timestamps = [float(f.split(".")[0]) for f in chunkfiles]
                dates = [time.asctime(time.gmtime(f)) for f in timestamps]
                prev_ts = None
                for i, cf in enumerate(chunkfiles):
                    ts = timestamps[i]
                    diff = ts - prev_ts if prev_ts is not None else None
                    prev_ts = ts
                    yield db_name, sensor, sensorpath, cf, ts, diff, dates[i]

    def _count_chunks(self, filter_db_name=None):
        """Fast count of all chunks without allocating any per-chunk structures."""
        n = 0
        for _ in self._iter_chunks(filter_db_name):
            n += 1
        return n

    def verify_all_chunks(
        self,
        filter_db_name=None,
        dtype=DTYPE,
        print_per_chunk_data=True,
        quiet=False,
        progress=False,
    ):
        """Gives some statistics on the chunks of a certain DB (or all if not db_name).

        Corrupt chunks (those that cannot be parsed as a numpy array of the
        expected shape) are logged and collected; the function does not raise
        on corruption so the user can scan an entire database in one pass.

        Args
            filter_db_name : if set, only this database is scanned.
            dtype : numpy dtype to use when reading chunks (default DTYPE).
            print_per_chunk_data : if True, the pprint summary of every valid
                chunk is printed (verbose).
            quiet : if True, suppress the per-chunk output and the per-sensor
                summary. Only the corrupt-chunk report is printed at the end
                (if any). The function always returns the corrupt list
                regardless of this flag.
            progress : if True, show a progress bar (tqdm if installed, else
                a stdlib fallback) and suppress per-chunk / per-sensor output.
                Mutually compatible with quiet: setting progress implies
                quiet. The corrupt report is always printed at the end.

        Returns
            list of tuples (filepath, error_message, prev_diff, date) for each
            corrupt chunk discovered.
        """
        if progress:
            quiet = True

        # Count total chunks for the progress bar without materialising
        # metadata for every chunk in memory.
        total_chunks = self._count_chunks(filter_db_name) if progress else None
        bar = _make_progress_bar(total_chunks) if progress else None

        # Process all chunks via streaming generator
        corrupt = []
        last_sensor = (None, None)
        sensor_total = 0
        sensor_count = 0
        try:
            for db_name, sensor, sensorpath, cf, ts, diff, date in self._iter_chunks(
                filter_db_name
            ):
                fpath = self.path(sensorpath, cf)
                if not quiet and (db_name, sensor) != last_sensor:
                    if last_sensor != (None, None):
                        print()
                        print(
                            f"Summary for db_name={last_sensor[0]} "
                            f"sensor={last_sensor[1]}"
                        )
                        print(f"Number of chunks: {sensor_count}")
                        print(f"Number of used rows: {sensor_total}")
                        print()
                    print(f"--- {db_name}/{sensor} ---")
                    last_sensor = (db_name, sensor)
                    sensor_total = 0
                    sensor_count = 0
                if not quiet:
                    print("{} - {} - {}".format(ts, diff, date))
                try:
                    stat = self.__verify_chunk_content(
                        fpath,
                        dtype=dtype,
                        print_summary_stats=print_per_chunk_data and not quiet,
                    )
                    sensor_total += stat["rows_used"]
                except ValueError as e:
                    logger.error(f"Corrupt chunk: {fpath} -- {e}")
                    corrupt.append((fpath, str(e), diff, date))
                sensor_count += 1
                if bar is not None:
                    bar.update(1)
        finally:
            if bar is not None:
                bar.close()

        if not quiet and last_sensor != (None, None):
            print()
            print(f"Summary for db_name={last_sensor[0]} sensor={last_sensor[1]}")
            print(f"Number of chunks: {sensor_count}")
            print(f"Number of used rows: {sensor_total}")
            print()

        if corrupt:
            print(f"\n=== Found {len(corrupt)} corrupt chunk(s) ===")
            for fpath, msg, diff, date in corrupt:
                print(f"  {fpath}  (ts={date}, prev_diff={diff})")
                print(f"    {msg}")
        return corrupt

    def get_open_func(self, filename):
        """Returns a function to open a chunk file in read-binary mode.

        The compression is auto-detected from the filename extension:
            * ``.zst`` -> zstandard stream reader
            * ``.gz``  -> gzip.open
            * anything else -> standard ``open``

        Old gzip chunks (default up to 0.8.x) and new zstd chunks
        (default since 0.9.0) can coexist in the same database and
        are both read transparently.
        """
        if filename.endswith(COMPRESSION_ZSTD):
            if not _zstd_present():
                raise RuntimeError(
                    f"Cannot read {filename!r}: it is zstd-compressed but the "
                    "'zstandard' package is not installed. Install it with "
                    "'pip install zstandard' or migrate the chunk to gzip."
                )
            return lambda p, m: _zstd_decompress_stream(open(p, m))
        if filename.endswith(COMPRESSION_GZIP):
            return gzip.open
        return open

    def fast_read_np(self, filename, shape=None, dtype=DTYPE):
        """Reads a chunk file into a numpy array.

        Raises ValueError with a detailed message (path, expected size, actual
        size, missing bytes) if the file is corrupt: a chunk whose byte size
        does not match the expected `CHUNK_ROWS * (n_columns + 2) * itemsize`.
        """
        if not os.path.isfile(filename):
            return None

        open_func = self.get_open_func(filename)
        itemsize = np.dtype(dtype).itemsize

        with open_func(filename, "rb") as f:
            buff = f.read()
        arr = np.frombuffer(buff, dtype=dtype)

        if shape is None:
            n_cols = _get_chunkcolumns(filename)
            # The number of rows is fixed by CHUNK_ROWS, not by the file's
            # current byte count (a truncated file would yield a wrong
            # row count if we divided). We trust the constant and validate
            # the file size against it.
            shape = (CHUNK_ROWS, n_cols)

        expected = shape[0] * shape[1]

        if arr.shape[0] == expected:
            arr.shape = shape
            return arr

        if arr.shape[0] == 0 and shape == (CHUNK_ROWS, _get_chunkcolumns(filename)):
            # Empty file with an implicit (auto-detected) shape: preserve
            # the original behavior of returning a full-shape NaN array.
            # Callers filter empty rows by checking `positions > 0`.
            return np.full(shape, np.nan, dtype=dtype)

        # Otherwise the file is corrupt: report a rich error.
        bytes_expected = expected * itemsize
        bytes_actual = arr.shape[0] * itemsize
        raise ValueError(
            f"Corrupt chunk {filename!r}: expected {expected} elements "
            f"({shape[0]} rows x {shape[1]} cols, dtype={np.dtype(dtype).name}, "
            f"{bytes_expected} bytes), got {arr.shape[0]} elements "
            f"({bytes_actual} bytes). Missing {expected - arr.shape[0]} elements "
            f"({bytes_expected - bytes_actual} bytes). Likely a truncated write."
        )

    def fast_read_np_partial(self, filename, dtype=DTYPE):
        """Reads a chunk file even if it is truncated, returning only the
        complete rows that fit in the available bytes.

        Trailing bytes that do not complete a full row are silently
        discarded (this is the desired behaviour for repairing truncated
        chunks: the partial row at the cut point cannot be trusted and
        should be re-derived or dropped).

        Returns:
            (arr, n_rows, n_cols) where arr is a numpy array of shape
            (n_rows, n_cols). Returns (None, 0, 0) if the file does not
            exist or cannot be read.
        """
        if not os.path.isfile(filename):
            return None, 0, 0
        try:
            n_cols = _get_chunkcolumns(filename)
        except ValueError:
            return None, 0, 0
        itemsize = np.dtype(dtype).itemsize
        open_func = self.get_open_func(filename)
        try:
            with open_func(filename, "rb") as f:
                buff = f.read()
        except (OSError, gzip.BadGzipFile, EOFError):
            return None, 0, 0

        row_size = n_cols * itemsize
        if row_size <= 0:
            return None, 0, 0
        n_rows = len(buff) // row_size
        if n_rows == 0:
            return np.empty((0, n_cols), dtype=dtype), 0, n_cols
        complete_bytes = n_rows * row_size
        arr = np.frombuffer(buff[:complete_bytes], dtype=dtype)
        arr.shape = (n_rows, n_cols)
        return arr, n_rows, n_cols

    def repair_corrupt_chunks(
        self,
        corrupt_list,
        backup=True,
        dry_run=False,
        checksum_atol=1e-3,
    ):
        """Attempts to repair truncated chunks by reading the complete
        rows, verifying their checksums, and extending the file with
        NaN-filled rows up to CHUNK_ROWS.

        For each chunk in `corrupt_list` (a tuple whose first element
        is the filepath, as returned by `verify_all_chunks`):

          1. Reads the complete rows with `fast_read_np_partial` (the
             trailing partial row, if any, is discarded).
          2. If any of the recovered rows contain data (position > 0),
             validates that the stored checksum (last column) matches
             `nansum(metric_columns)`. If the check fails, the file is
             left untouched and the result is `skipped_checksum`.
          3. If checksums are OK (or there are no data rows to
             validate), the file is rewritten with shape
             (CHUNK_ROWS, n_cols) where the recovered rows are copied
             at the top and the remaining rows are NaN.
          4. If `backup=True`, the original file is renamed to
             `<file>.corrupt.bak` before the new file is written.
          5. The write goes through `safe_createfile`, which uses
             `<file>.tmp.<pid>.<id>` + `os.replace` for atomicity.

        Args:
            corrupt_list: iterable of tuples from `verify_all_chunks`.
            backup: if True, keep the original file at `<file>.corrupt.bak`.
            dry_run: if True, do not write anything; just report.
            checksum_atol: tolerance passed to `np.isclose` when
                validating checksums of recovered data rows.

        Returns:
            list of (filepath, status, detail) with status in
            {"repaired", "would_repair", "skipped_checksum",
             "skipped_unreadable"}.
        """
        results = []
        for entry in corrupt_list:
            fpath = entry[0]
            arr, n_rows, n_cols = self.fast_read_np_partial(fpath)
            if arr is None:
                results.append((fpath, "skipped_unreadable", "could not read file"))
                continue

            # Validate checksums of any data rows (position > 0). NaN-only
            # rows are skipped -- they are the natural state of unwritten
            # rows in a chunk and the checksum of a NaN row is also NaN,
            # which would always fail the isclose check.
            data_mask = arr[:, 0] > 0
            if data_mask.any():
                v = arr[data_mask, 1:-1]
                expected = np.ma.masked_array(v, np.isnan(v)).sum(axis=1)
                actual = arr[data_mask, -1]
                if not np.isclose(expected, actual, atol=checksum_atol).all():
                    bad_idx = np.where(
                        ~np.isclose(expected, actual, atol=checksum_atol)
                    )[0]
                    results.append(
                        (
                            fpath,
                            "skipped_checksum",
                            f"{len(bad_idx)} data row(s) have invalid checksum",
                        )
                    )
                    continue

            missing = CHUNK_ROWS - n_rows
            if dry_run:
                results.append(
                    (
                        fpath,
                        "would_repair",
                        f"would keep {n_rows} row(s), add {missing} NaN row(s)",
                    )
                )
                continue

            # Build the full chunk: recovered rows at the top, NaN padding
            full = np.full((CHUNK_ROWS, n_cols), np.nan, dtype=DTYPE)
            if n_rows > 0:
                full[:n_rows] = arr

            if backup:
                backup_path = fpath + ".corrupt.bak"
                if os.path.exists(backup_path):
                    # Don't clobber a previous backup by accident; remove
                    # the older one explicitly so the chain is recoverable.
                    os.remove(backup_path)
                os.rename(fpath, backup_path)

            # Atomic write
            try:
                with self.safe_createfile(fpath, "wb") as f:
                    f.write(full.tobytes())
            except Exception as e:
                # Try to restore the backup so the user is not left
                # without their data.
                if backup and os.path.exists(backup_path):
                    try:
                        if os.path.exists(fpath):
                            os.remove(fpath)
                        os.rename(backup_path, fpath)
                    except OSError:
                        pass
                results.append((fpath, "skipped_unreadable", f"write failed: {e}"))
                continue

            results.append(
                (
                    fpath,
                    "repaired",
                    f"kept {n_rows} row(s), padded {missing} NaN row(s)",
                )
            )
        return results

    def _swap_extension(self, path: str, new_ext: str) -> str:
        """Return ``path`` with its extension replaced by ``new_ext``.
        If ``path`` has no extension, ``new_ext`` is appended.
        """
        base, _ = os.path.splitext(path)
        if new_ext:
            return base + new_ext
        return base

    def _compress_bytes(self, data: bytes, ext: str, level: int = 3) -> bytes:
        """Return ``data`` compressed with the codec selected by ``ext``."""
        if ext == COMPRESSION_ZSTD:
            return _zstd_compress(data, level=level)
        if ext == COMPRESSION_GZIP:
            return gzip.compress(data, compresslevel=6)
        if ext:
            raise ValueError(f"Unknown compression extension: {ext!r}")
        return data  # no compression

    def migrate_compression(
        self,
        chunk_list,
        target_ext: str = COMPRESSION_ZSTD,
        backup: bool = True,
        dry_run: bool = False,
        zstd_level: int = 3,
        checksum_atol: float = 1e-3,
        base_dir: str = None,
    ):
        """Re-write each chunk in ``chunk_list`` using the codec selected
        by ``target_ext``.

        This is how an existing database migrates from gzip to zstd
        (the new default since 0.9.0) without losing data:

        1. Read the chunk in its current format (auto-detected by
           extension).
        2. Validate checksums of the recovered data rows. If any row
           has an invalid checksum, the file is left untouched and the
           result is ``skipped_checksum``.
        3. Re-compress the data with the target codec and write
           atomically (via :py:meth:`safe_createfile`) to a sibling
           file with the new extension. The original is renamed to
           ``<file>.<ext>.bak`` first (unless ``backup=False``).
        4. On write failure, the backup is restored so the user is not
           left without their data.

        Chunks whose current extension already matches ``target_ext``
        are reported as ``skipped_already_target`` and left alone.

        Args:
            chunk_list: iterable of tuples from
                :py:meth:`getchunks` or :py:meth:`verify_all_chunks`
                (the first element is the filepath).
            target_ext: one of ``COMPRESSION_ZSTD``, ``COMPRESSION_GZIP``,
                or empty string for uncompressed.
            backup: if True, keep the original at
                ``<file>.<ext>.bak``.
            dry_run: if True, do not write anything; only report.
            zstd_level: zstd level 1-22 (default 3; see the benchmark).
            checksum_atol: tolerance for the isclose check on recovered
                data rows.

        Returns:
            list of ``(filepath, status, detail)`` with status in
            ``{"migrated", "would_migrate", "skipped_already_target",
            "skipped_checksum", "skipped_unreadable"}``.
        """
        if target_ext not in (COMPRESSION_ZSTD, COMPRESSION_GZIP, ""):
            raise ValueError(
                f"target_ext must be one of '', {COMPRESSION_ZSTD!r}, "
                f"{COMPRESSION_GZIP!r}; got {target_ext!r}"
            )
        if target_ext == COMPRESSION_ZSTD and not _zstd_present():
            raise RuntimeError(
                "Cannot migrate to zstd: the 'zstandard' package is not "
                "installed. Install it with 'pip install zstandard'."
            )

        results = []
        # Allow callers to pass just bare filenames (as returned by
        # getchunks) by joining them with the configured base_dir. We
        # only join when the file does not already exist as a relative
        # path, so callers that pass full paths are unaffected.
        bdir = base_dir or self.base_path

        def _resolve(entry_fpath):
            if os.path.isabs(entry_fpath) or os.path.exists(entry_fpath):
                return entry_fpath
            if bdir and os.path.dirname(entry_fpath) == "":
                return os.path.join(bdir, entry_fpath)
            return entry_fpath

        for entry in chunk_list:
            # Each entry can be either a bare path string (as returned
            # by getchunks or passed in by a CLI) or a tuple like
            # (filepath, status, ...) from verify_all_chunks. Accept
            # both shapes.
            entry_fpath = entry if isinstance(entry, str) else entry[0]
            fpath = _resolve(entry_fpath)
            current_ext = os.path.splitext(fpath)[1]
            if current_ext == target_ext:
                results.append(
                    (fpath, "skipped_already_target", current_ext or "(none)")
                )
                continue

            # Read in current format (auto-detected by extension)
            arr, n_rows, n_cols = self.fast_read_np_partial(fpath)
            if arr is None:
                results.append((fpath, "skipped_unreadable", "could not read file"))
                continue

            # Validate checksums of any data rows. NaN-only rows are
            # skipped (the checksum of a NaN row is also NaN, which
            # would always fail the isclose check). This is the same
            # rule used by repair_corrupt_chunks and by the read path.
            data_mask = arr[:, 0] > 0
            if data_mask.any():
                v = arr[data_mask, 1:-1]
                expected = np.ma.masked_array(v, np.isnan(v)).sum(axis=1)
                actual = arr[data_mask, -1]
                if not np.isclose(expected, actual, atol=checksum_atol).all():
                    bad_idx = np.where(
                        ~np.isclose(expected, actual, atol=checksum_atol)
                    )[0]
                    results.append(
                        (
                            fpath,
                            "skipped_checksum",
                            f"{len(bad_idx)} data row(s) have invalid checksum",
                        )
                    )
                    continue

            new_path = self._swap_extension(fpath, target_ext)
            src_size = os.path.getsize(fpath)
            if dry_run:
                results.append(
                    (
                        fpath,
                        "would_migrate",
                        f"would re-write as {target_ext or 'uncompressed'} "
                        f"(current {src_size} bytes)",
                    )
                )
                continue

            # Re-build the full chunk (NaN-pad if it was truncated)
            full = np.full((CHUNK_ROWS, n_cols), np.nan, dtype=DTYPE)
            if n_rows > 0:
                full[:n_rows] = arr
            payload = self._compress_bytes(full.tobytes(), target_ext, level=zstd_level)

            if backup:
                backup_path = fpath + ".bak"
                if os.path.exists(backup_path):
                    # Don't clobber a previous backup; remove the older
                    # one so the chain is recoverable.
                    os.remove(backup_path)
                os.rename(fpath, backup_path)
            else:
                # No backup requested: delete the source before writing
                # the new file so the directory is never in a state with
                # both formats coexisting for the same timestamp. If the
                # new-file write fails, the source is gone -- but the
                # user explicitly opted out of keeping it.
                if os.path.exists(fpath):
                    os.remove(fpath)

            # Atomic write of the new file
            try:
                with self.safe_createfile(new_path, "wb") as f:
                    f.write(payload)
            except Exception as e:
                # Try to restore the backup so the user is not left
                # without their data.
                if backup and os.path.exists(backup_path):
                    try:
                        if os.path.exists(fpath):
                            os.remove(fpath)
                        os.rename(backup_path, fpath)
                    except OSError:
                        pass
                results.append((fpath, "skipped_unreadable", f"write failed: {e}"))
                continue

            new_size = os.path.getsize(new_path)
            results.append(
                (
                    fpath,
                    "migrated",
                    f"{current_ext or '(none)'} -> {target_ext or 'uncompressed'}: "
                    f"{src_size} -> {new_size} bytes",
                )
            )
        return results
