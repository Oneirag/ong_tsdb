#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Jan  4 00:48:46 2017

@author: ongpi
"""

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
        Returns a file descriptor with correct owner and permissions
        Permissions are set to read+write for the user and group,
        no permissions for others

        Args
            path : string
                Full path of the file to open
            mode : string
                File open mode (Default: "w")
        Return
            File descriptor
        Raises
            OSError if path is not valid
        """
        f = self.get_open_func(path)(path, mode)
        self.__fix_permissions(path)
        return f

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
        self, filter_db_name=None, dtype=DTYPE, print_per_chunk_data=True
    ):
        """Gives some statistics on the chunks of a certain DB (or all if not db_name).

        Corrupt chunks (those that cannot be parsed as a numpy array of the
        expected shape) are logged and collected; the function does not raise
        on corruption so the user can scan an entire database in one pass.

        Returns
            list of tuples (filepath, error_message, prev_diff, date) for each
            corrupt chunk discovered.
        """
        total_data = 0
        corrupt = []
        for db_name in self.getdbs():
            if filter_db_name and db_name != filter_db_name:
                continue
            for sensor in self.getsensors(db_name):
                sensorpath = self.path(db_name, sensor)
                chunkfiles = _get_chunkfiles(sensorpath)
                timestamps = [float(f.split(".")[0]) for f in chunkfiles]
                dates = [time.asctime(time.gmtime(f)) for f in timestamps]
                for i in range(len(dates)):
                    if i > 0:
                        difference = timestamps[i] - timestamps[i - 1]
                    else:
                        difference = None
                    fpath = self.path(sensorpath, chunkfiles[i])
                    print("{} - {} - {}".format(timestamps[i], difference, dates[i]))
                    try:
                        stat = self.__verify_chunk_content(
                            fpath,
                            dtype=dtype,
                            print_summary_stats=print_per_chunk_data,
                        )
                        total_data += stat["rows_used"]
                    except ValueError as e:
                        logger.error(f"Corrupt chunk: {fpath} -- {e}")
                        corrupt.append((fpath, str(e), difference, dates[i]))
                print()
                print(f"Summary for db_name={db_name} sensor={sensor}")
                print(f"Number of chunks: {len(chunkfiles)}")
                print(f"Number of used rows: {total_data}")
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
