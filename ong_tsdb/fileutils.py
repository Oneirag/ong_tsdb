#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Jan  4 00:48:46 2017

@author: ongpi
"""
import time
import os
import re
import grp
import gzip
from pwd import getpwnam
import stat
import numpy as np
from ong_tsdb import config, BASE_DIR, COMPRESSION_EXT, logger, DTYPE, CHUNK_ROWS
from pprint import pprint


# Regular expression for parsing chunk filenames
re_chunk_filename = re.compile(f"(?P<timestamp>\d+).(?P<n_columns>\d+)(?P<compression>{COMPRESSION_EXT})?")


def extract_filename_parts(filename):
    """Returns named groups from filename using re_chunk_filename regular expression """
    return re_chunk_filename.match(os.path.basename(filename)).groupdict()


def generate_filename_from_parts(path, timestamp, n_columns, compression=""):
    """Creates a chunk filename from parts, where kwargs will be used to generate regular expression"""
    if not isinstance(timestamp, str):
        timestamp = f"{timestamp:.0f}"
    chunk_filename = f"{timestamp}.{n_columns}{compression}"
    return os.path.join(path, chunk_filename)


def _get_subdirs(path):
    """Returns the list of subdirs of current path"""
    return [n for n in os.listdir(path)
            if os.path.isdir(os.path.join(path, n))]


def _get_chunkfiles(path):
    """Returns a sorted list of chunk (either compressed or uncompressed files)"""
    files = [n for n in os.listdir(path) if os.path.isfile(os.path.join(path, n))
             and re_chunk_filename.match(n)
             ]
    files.sort()
    return files


def _get_chunkcolumns(filename):
    # Returns number of columns of the chunk from its filename.
    # Chunks have a "{timestamp}.{n_columns}[.gz]" name, so extracts the {size} part
    return int(extract_filename_parts(filename)['n_columns'])


class FileUtils(object):
    """
    Class to manage files and dirs with correct permissions
    """

    def __init__(self, base_path=BASE_DIR,
                 file_user=config("FILE_USER", os.getuid()),
                 file_group=config("FILE_GROUP", os.getgid())):
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
            self.groupid = file_group if isinstance(file_group, int) else grp.getgrnam(file_group).gr_gid
            self.userid = file_user if isinstance(file_group, int) else getpwnam(file_user).pw_uid
        except:
            raise KeyError(
                "User or Group {} does not exist. Create it with the setup script install.sh".format(file_group))

        self.__path = os.path.abspath(base_path or "..")
        if not os.path.isdir(base_path):
            admin_token = config("admin_token", None)
            if admin_token is None:
                raise Exception('Database cannot be created, add "admin_token" to your configuration file')
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
        stat_mode = stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP  # This is 0o600 in octal and 384 in decimal.
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

        # TODO: fix permissions
        f = self.get_open_func(path)(path, mode)
        self.__fix_permissions(path)
        return f

        # This works, but safer it not used...
        flags = os.O_WRONLY | os.O_CREAT  # | os.O_EXCL  # Refer to "man 2 open".
        stat_mode = stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP  # This is 0o600 in octal and 384 in decimal.

        #        f = open(path, mode)
        #        return f
        # Open file descriptor
        umask_original = os.umask(0)
        try:
            fdesc = os.open(path, flags, stat_mode)
        finally:
            os.umask(umask_original)
        fdesc = os.open(path, flags, stat_mode)
        os.fchmod(fdesc, stat_mode)
        os.fchown(fdesc, self.userid, self.groupid)
        return os.fdopen(fdesc, mode)

    def __verify_chunk_content(self, filename, dtype=DTYPE, print_summary_stats=True):
        """Prints to screen the analysis of the chunk file filename"""
        arr = self.fast_read_np(filename, dtype=dtype)
        if arr.shape[0] != CHUNK_ROWS:
            logger.error(f"Error in {filename}: expected {CHUNK_ROWS} rows but file has {arr.shape[0]}")
        index = arr[:, 0].nonzero()[0]
        min_index = index[0] if len(index) > 0 else -1
        max_index = index[-1] if len(index) > 0 else -1
        stat = dict(filename=filename,
                    rows_total=len(arr),
                    rows_used=len(index),
                    rows_used_ratio_pct=len(index) / float(len(arr)) * 100,
                    row_index_min=min_index, row_index_max=max_index,
                    ratio_max_index=(max_index + 1) / float(len(arr))
                    )
        if print_summary_stats:
            pprint(stat)
        return stat

    def verify_all_chunks(self, filter_db_name=None, dtype=DTYPE, print_per_chunk_data=True):
        """Gives some statistics on the chunks of a certain DB (or all if not db_name)"""
        total_data = 0
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
                    print("{} - {} - {}".format(timestamps[i], difference, dates[i]))
                    stat = self.__verify_chunk_content(self.path(sensorpath, chunkfiles[i]), dtype=dtype,
                                                       print_summary_stats=print_per_chunk_data)
                    total_data += stat["rows_used"]
                print()
                print(f"Summary for {db_name=} {sensor=}")
                print(f"Number of chunks: {len(chunkfiles)}")
                print(f"Number of used rows: {total_data}")
                print()

    def get_open_func(self, filename):
        """Returns function to open file. If file is compressed uses gzip.open else uses standard open"""
        if filename.endswith(COMPRESSION_EXT):
            # return bz2.open
            return gzip.open
        else:
            return open

    def fast_read_np(self, filename, shape=None, dtype=DTYPE):
        """Reads a chunk file into a numpy array"""
        if not os.path.isfile(filename):
            return None

        open_func = self.get_open_func(filename)

        # arr = np.fromfile(filename, dtype=dtype)
        with open_func(filename, "rb") as f:
            buff = f.read()
        arr = np.frombuffer(buff, dtype=dtype)

        if shape is None:
            # The size in bytes must be turned into a numpy array, correcting by numpy columns and dtype size
            n_cols = _get_chunkcolumns(filename)
            shape = int(arr.shape[0] / n_cols), n_cols

        arr.shape = shape
        return arr


if __name__ == "__main__":
    # FU = FileUtils(".")
    # FILENAME = os.path.join(os.path.abspath(os.curdir),
    #                         "ejemplo.txt")
    # with FU.safe_createfile(FILENAME, "w") as f:
    #     f.write("hola")
    # print("File {} created".format(FILENAME))
    FU = FileUtils()
    FU.verify_all_chunks(dtype=np.float32)
