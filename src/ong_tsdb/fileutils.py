#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Jan  4 00:48:46 2017

@author: ongpi
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
from ong_tsdb import config, BASE_DIR, COMPRESSION_EXT, logger, DTYPE, CHUNK_ROWS
from pprint import pprint


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


# Regular expression for parsing chunk filenames. Anchored at end (\Z) so that
# leftover or renamed files (e.g. "1234.5.gz.bak") do not partially match and
# silently produce wrong metadata.
re_chunk_filename = re.compile(
    rf"(?P<timestamp>\d+)\.(?P<n_columns>\d+)(?P<compression>{re.escape(COMPRESSION_EXT)})?\Z"
)


def extract_filename_parts(filename):
    """Returns named groups from filename using re_chunk_filename regular expression.

    Raises ValueError if the filename does not match the chunk pattern.
    """
    m = re_chunk_filename.fullmatch(os.path.basename(filename))
    if m is None:
        raise ValueError(
            f"Filename {filename!r} does not match chunk pattern "
            f"(expected '<timestamp>.<n_columns>[.gz]')"
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
        if not os.path.isdir(base_path):
            admin_token = config("admin_token", None)
            if admin_token is None:
                raise Exception(
                    'Database cannot be created, add "admin_token" to your configuration file'
                )
            # Create root dir
            os.makedirs(base_path)
            self.__path = os.path.abspath(base_path)
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

        # Phase 1: discover all chunks so we know the total for the progress
        # bar. We do this in one pass and process in a second so the bar
        # can show real progress.
        sensors_chunks = []
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
                chunks = []
                for i, cf in enumerate(chunkfiles):
                    diff = timestamps[i] - timestamps[i - 1] if i > 0 else None
                    chunks.append(
                        {
                            "cf": cf,
                            "diff": diff,
                            "date": dates[i],
                            "ts": timestamps[i],
                        }
                    )
                sensors_chunks.append(
                    {
                        "db": db_name,
                        "sensor": sensor,
                        "sensorpath": sensorpath,
                        "chunks": chunks,
                    }
                )

        total_chunks = sum(len(s["chunks"]) for s in sensors_chunks)
        bar = _make_progress_bar(total_chunks) if progress else None

        # Phase 2: process all chunks
        corrupt = []
        last_sensor = (None, None)
        sensor_total = 0
        sensor_count = 0
        try:
            for s in sensors_chunks:
                db_name = s["db"]
                sensor = s["sensor"]
                sensorpath = s["sensorpath"]
                for c in s["chunks"]:
                    fpath = self.path(sensorpath, c["cf"])
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
                        print("{} - {} - {}".format(c["ts"], c["diff"], c["date"]))
                    try:
                        stat = self.__verify_chunk_content(
                            fpath,
                            dtype=dtype,
                            print_summary_stats=print_per_chunk_data and not quiet,
                        )
                        sensor_total += stat["rows_used"]
                    except ValueError as e:
                        logger.error(f"Corrupt chunk: {fpath} -- {e}")
                        corrupt.append((fpath, str(e), c["diff"], c["date"]))
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
        """Returns function to open file. If file is compressed uses gzip.open else uses standard open"""
        if filename.endswith(COMPRESSION_EXT):
            # return bz2.open
            return gzip.open
        else:
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


if __name__ == "__main__":
    # FU = FileUtils(".")
    # FILENAME = os.path.join(os.path.abspath(os.curdir),
    #                         "ejemplo.txt")
    # with FU.safe_createfile(FILENAME, "w") as f:
    #     f.write("hola")
    # print("File {} created".format(FILENAME))
    FU = FileUtils()
    FU.verify_all_chunks(dtype=np.float32)
