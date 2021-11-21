#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
"""
Class to manage file storage (under BASE_DIR directory)
"""
import enum
import hashlib
import inspect
import os
import random
import shutil
import string
import time
import threading

import numpy as np
import pandas as pd
import ujson
from six.moves._thread import start_new_thread

from ong_tsdb import logger, LOCAL_TZ, BASE_DIR, DTYPE
from ong_tsdb.chunker import Chunker
from ong_tsdb.fileutils import FileUtils, re_chunk_filename


class OngTSDBbBaseException(Exception):
    """Base class for exceptions of this module"""
    pass


class NotAuthorizedException(OngTSDBbBaseException):
    pass


class ElementAlreadyExistsException(OngTSDBbBaseException):
    pass


class ElementNotFoundException(OngTSDBbBaseException):
    pass


class InvalidDataWriteException(OngTSDBbBaseException):
    pass


class Actions(enum.Enum):
    CREATE = 1
    READ = 2
    WRITE = 3


class OngTSDB(object):
    __FREQ_KEY = "Freq"
    __METRICS_KEY = "Metrics"
    __READ_KEY = "Read_Key"
    __WRITE_KEY = "Write_key"

    _lock = threading.Lock()

    def __init__(self, path=BASE_DIR):
        """Inits database in the path (defaults to BASE_DIR). There must be a CONFIG.JS in the path,
        otherwise it will be created with a new admin password that will be shown with logger.info"""
        self.FU = FileUtils(path)
        if not os.path.isfile(self.FU.path_config()):
            os.makedirs(BASE_DIR)
            FU = FileUtils()
            length = 20
            admin_key = ''.join(random.sample(string.hexdigits, int(length)))
            with FU.safe_createfile(FU.path_config(), "w") as f:
                f.write(admin_key)
            logger.info("DB correctly setup")
            logger.info("Admin key is")
            logger.info("=" * length)
            logger.info(admin_key)
            logger.info("=" * length)
            logger.info("You can check admin key in {}".format(FU.path_config()))

        with open(self.FU.path_config(), "r") as f:
            self.admin_key = f.readline()
        self.config_reload()

    def config_reload(self):
        """Reloads configuration for DataBase (databases, metrics, keys)"""
        sdb = dict()
        for db in self.FU.getdbs():
            sdb[db] = dict()
            for sensor in self.FU.getsensors(db):
                try:
                    data = []
                    with open(self.FU.path_config(db, sensor)) as f:
                        data = ujson.load(f)
                except:
                    pass
                sdb[db][sensor] = data
        self.db = sdb

    def __create_internal_structure(self, path, check_list, json_string=None):
        file_path = self.FU.path_config(path)
        if path not in check_list:
            self.FU.safe_makedirs(path)
            if json_string is not None:
                with self.FU.safe_createfile(file_path, "w") as f:
                    f.write(json_string)
        else:
            if json_string is not None:
                with open(file_path, "r") as f:
                    if f.read() != json_string:
                        raise ElementAlreadyExistsException("Element already exists with different config")

    def _check_auth(self, key, action, db, sensor):
        """Checks if key is valid for the action in sensor. Raises exception otherwise"""
        if key == self.admin_key:
            return  # admin key is valid for all actions
        is_writekey = self.db[db][sensor][self.__WRITE_KEY] == key
        if is_writekey and action in (Actions.WRITE, Actions.READ):
            return
        is_readkey = self.db[db][sensor][self.__READ_KEY] == key
        if is_readkey and action in (Actions.READ,):
            return
        time.sleep(random.random() * .01)  # This random prevents performing time attacks
        raise NotAuthorizedException("Invalid key for function " +
                                     inspect.stack()[1][3])

    def existdb(self, key, db):
        """True id db exists"""
        return db in self.db
        # if db not in self.db.keys():
        #     if not os.path.isdir(self.FU.path(db)):
        #         return False
        # return True

    def existsensor(self, key, db, sensor):
        """Checks if a sensor exists in database"""
        self._check_auth(key, Actions.READ, db, sensor)
        if not self.existdb(key, db) or sensor not in self.db[db].keys() or \
                not os.path.isdir(self.FU.path(db, sensor)):
            return False
        return True

    def createdb(self, admin_key, db):
        self._check_auth(admin_key, Actions.CREATE, None, None)
        if self.existdb(admin_key, db):
            raise ElementAlreadyExistsException(f"Database {db} already exists")
        self.__create_internal_structure(
            self.FU.path(db),
            self.db.keys())
        self.db[db] = dict()

    def deletedb(self, admin_key, db):
        self._check_auth(admin_key, Actions.CREATE, db, None)
        if self.existdb(admin_key, db):
            shutil.rmtree(self.FU.path(db))
            if db in self.db:
                self.db.pop(db)
            return True
        else:
            return False

    def deletesensor(self, admin_key, db, sensor):
        self._check_auth(admin_key, Actions.CREATE, db, sensor)
        if self.existsensor(admin_key, db, sensor):
            shutil.rmtree(self.FU.path(db, sensor))
            self.db[db].pop(sensor)
            return True
        else:
            return False

    def createsensor(self, admin_key, db, sensor, period, write_key, read_key, metrics, force_update=False):
        """
        Creates a new sensor in the db (a directory for the sensor with its CONFIG.JSON file)
        :param admin_key: admin key to create sensor
        :param db: db name
        :param sensor: sensor name
        :param period: period of the sensor as ("1s", "5m", "2h"...)
        :param write_key:
        :param read_key:
        :param metrics: list of numeric metrics of the sensor
        :param force_update: True to overwrite structure. Defaults to False (and raises exception if sensor exists)
        :return:
        """
        self._check_auth(admin_key, Actions.CREATE, db, sensor)
        if not self.existsensor(admin_key, db, sensor) or force_update:
            # Create only if does not exist
            _ = Chunker(period)  # Will raise exception if invalid period
            config = {
                self.__WRITE_KEY: write_key,
                self.__READ_KEY: read_key,
                self.__FREQ_KEY: period,
                self.__METRICS_KEY: metrics
            }
            self.__create_internal_structure(
                self.FU.path(db, sensor),
                self.db[db].keys(),
                ujson.dumps(config)
            )
            self.db[db][sensor] = config
        else:
            raise ElementAlreadyExistsException(f"Sensor {sensor} already exist in {db}")

    def getmetrics(self, key, db, sensor, force_reload=False):
        """Returns the list of metrics of a certain db and sensor"""
        if force_reload:
            self.config_reload()
        return self._getmetadata(key, db, sensor, self.__METRICS_KEY)

    def _getmetadata(self, key, db, sensor, field):
        # self._check_auth(key, Actions.READ, db, sensor)
        # if self.existsensor(key, db, sensor):
        return self.db[db][sensor][field]

    # else:
    #    raise ElementNotFoundException(f"Db {db} or sensor {sensor} not found")

    def getnumpyrow(self, key, db, sensor):
        return np.zeros((1, len(self.getmetrics(key, db, sensor))))

    def __getrecordsize(self, key, db, sensor):
        """Returns record size in bytes"""
        return np.dtype(DTYPE).itemsize * self.__getarraysize(key, db, sensor)

    def __getarraysize(self, key, db, sensor):
        """Returns record size in columns of the np.array writen in the chunks"""
        return 2 + len(self.getmetrics(key, db, sensor))

    def _replace_chunk(self, db, sensor, original_chunk_name, new_chunk_name, new_array=None, compressed=False):
        """
        Replaces a chunk with a new one (that can be the compressed version or adding additional columns)
        :param db: data base name
        :param sensor: sensor name
        :param new_chunk_name: name (without path) of the new chunk
        :param new_array: array (uncompressed) that will be written. If none, the array in original_chunk will be used
        :param compressed: True if file wil be compressed. Defaults to false
        :return: None
        """
        if new_array is None:
            new_array = self.FU.fast_read_np(self.get_FU_path(db, sensor, original_chunk_name), dtype=DTYPE)
        if compressed:
            raise NotImplementedError("Compression not implemented")
        f = self.FU.safe_createfile(self.get_FU_path(db, sensor, new_chunk_name), "wb")
        f.write(new_array.tobytes())
        f.close()
        # Delete old chunk and keep just new one
        os.remove(self.get_FU_path(db, sensor, original_chunk_name))

    def add_new_metrics(self, key, db, sensor, new_metrics: list):
        """Adds new metric(s) to a sensor. To so, opens all chunks from the changing chunk ahead,
        adds empty column to all of then, changes the column name an deletes the old ones"""

        # Open all chunks after timestamp
        chunker = self.getchunker(key, db, sensor)
        # start_ts = str(int(chunker.init_date(timestamp)))
        all_chunks = self.FU.getchunks(db, sensor)  # Names without path
        # chunks_to_change = [c for c in self.FU.getchunks(db, sensor) if c > start_ts]     # Names without path
        chunks_to_change = all_chunks
        metrics = self.getmetrics(key, db, sensor)
        period = self.db[db][sensor][self.__FREQ_KEY]
        write_key = self.db[db][sensor][self.__WRITE_KEY]
        read_key = self.db[db][sensor][self.__READ_KEY]
        self.createsensor(self.admin_key, db, sensor, period, write_key, read_key, metrics + new_metrics,
                          force_update=True)
        self.config_reload()
        updated_metrics = self.getmetrics(key, db, sensor)
        for old_chunk_name in chunks_to_change:
            a = self.FU.fast_read_np(self.get_FU_path(db, sensor, old_chunk_name), dtype=DTYPE)
            new_array = np.concatenate((a[:, :-1],
                                        np.zeros((a.shape[0], len(new_metrics)), dtype=a.dtype),
                                        a[:, -1][:, None]),
                                       axis=1)
            parts = re_chunk_filename.match(old_chunk_name).groupdict()
            compressed = parts['compression'] is not None
            new_chunk_name = chunker.chunk_name(int(parts['timestamp']),
                                                int(parts['n_columns']) + len(new_metrics),
                                                compressed)
            self._replace_chunk(db, sensor, old_chunk_name, new_chunk_name, new_array, compressed)
        pass

    # This function is not used anymore
    # def writetick(self, key, db, sensor, value_array: np.array, value_indexes: list = None, timestamp=None):
    #     """
    #     Writes tick data into database
    #     :param key: key for authentication
    #     :param db: name of database
    #     :param sensor: name of sensor
    #     :param value_array: a numpy array with the data to write. Important: its dtype will be used for writing all
    #     data in the chuck
    #     :param value_indexes: if None (default) value_array is a whole line in the vector, therefore its length will be
    #     checked against the length of the metrics of the sensor and if they don't match an exception will be risen.
    #     Otherwise, offset must be a list of indexes for each value in the array indicating the pos of the data to be
    #      writen
    #     :param timestamp: optional, timestamp (millis) of the current data. If None, then current timestamp will be used
    #     :return: None
    #     """
    #     self._check_auth(key, Actions.WRITE, db, sensor)
    #     self.existsensor(key, db, sensor)
    #     if value_indexes is None:
    #         if value_array.shape[1] != len(self.getmetrics(key, db, sensor)):
    #             raise InvalidDataWriteException("Invalid number of cols of numpy array")
    #     else:
    #         if len(value_indexes) != value_array.shape[1]:
    #             raise InvalidDataWriteException("Invalid number of indexes of numpy array")
    #     if timestamp is None:
    #         timestamp = time.time()
    #     bytes_chunk_record = self.__getrecordsize(key, db, sensor)
    #     cols_chunk_array = self.__getarraysize(key, db, sensor)
    #     chunker = Chunker(self._getmetadata(key, db, sensor, self.__FREQ_KEY))
    #     chunk_name = self.FU.path(db, sensor, chunker.chunk_name(timestamp, cols_chunk_array))
    #     pos = chunker.getpos(timestamp)
    #     # print(f"Writen in chunk: {chunk_name=} {pos=} {timestamp=}")
    #     with self._lock:
    #         if not os.path.isfile(chunk_name):
    #             f = self.FU.safe_createfile(chunk_name, 'wb')
    #             f.write(bytes(chunker.n_rows_per_chunk * bytes_chunk_record))
    #             f.seek(0)
    #         else:
    #             f = open(chunk_name, 'rb')
    #         f.seek(pos * bytes_chunk_record)
    #         if value_indexes is not None:  # offset == 0 when writing a whole tick
    #             value_write = f.read(bytes_chunk_record)
    #             value_write = np.fromstring(value_write, dtype=value_array.dtype)
    #             # value_write[0] = timestamp
    #             value_write[0] = pos + 1
    #             # Add one due as the first column is the timestamp
    #             value_write[list(v + 1 for v in value_indexes)] = value_array
    #             value_write[-1] = value_write[1:-1].sum()  # [1:-2] does not work...curious!
    #             f.seek(pos * bytes_chunk_record)
    #         else:
    #             value_write = np.column_stack((
    #                 # timestamp,
    #                 pos + 1,
    #                 value_array,
    #                 value_array.sum(1))).astype(value_array.dtype)
    #         f.write(value_write.tobytes())
    #         f.close()

    def writetick_numpy(self, key, db, sensor, np_values: np.array, np_timestamps=None):
        """
        Writes a numpy array of tick data into database
        :param key: key for authentication
        :param db: name of database
        :param sensor: name of sensor
        :param np_values: a numpy array with the data to write. Important: its dtype will be used for writing all
        data in the chuck
        :param np_timestamps: optional, a vector of timestamps (nanos) of the current data.
        :return: None
        """
        self._check_auth(key, Actions.WRITE, db, sensor)
        self.existsensor(key, db, sensor)
        if np_values.shape[1] != len(self.getmetrics(key, db, sensor)):
            raise InvalidDataWriteException("Invalid number of cols of numpy array")
        # Give default value to timestamp and convert to millis from nanos
        if np_timestamps is None:
            np_timestamps = time.time() * np.ones((np_values, 1))
        cols_chunk_array = self.__getarraysize(key, db, sensor)
        chunker = self.getchunker(key, db, sensor)
        chunk_name = self.FU.path(db, sensor, chunker.chunk_name(np_timestamps[0], cols_chunk_array))
        pos = chunker.getpos(np_timestamps)
        # print(f"Writen in chunk: {chunk_name=} {pos=} {timestamp=}")
        with self._lock:
            if not os.path.isfile(chunk_name):
                f = self.FU.safe_createfile(chunk_name, 'wb')
                value_write = np.zeros((chunker.n_rows_per_chunk, cols_chunk_array), dtype=np_values.dtype)
            else:
                # Open for read only
                f = self.FU.get_open_func(chunk_name)(chunk_name, 'rb')
                value_write = np.fromstring(f.read(), dtype=np_values.dtype)
                value_write.shape = (chunker.n_rows_per_chunk, cols_chunk_array)
                f.close()
                # Reopen for writing
                f = self.FU.get_open_func(chunk_name)(chunk_name, 'wb')

            idx_not_nan = np.nonzero(~np.isnan(np_values))  # Write only not nan values
            value_write[pos[idx_not_nan[0]], idx_not_nan[1] + 1] = np_values[idx_not_nan]
            value_write[pos, -1] = value_write[pos, 1:-1].sum(axis=1)
            value_write[pos, 0] = pos + 1
            f.write(value_write.tobytes())
            f.close()

    def np2pd(self, key, db, sensor, dates, values, tz=LOCAL_TZ):
        dateindex = pd.to_datetime(dates, unit='s', utc=True).tz_convert(tz)
        return pd.DataFrame(values, index=dateindex,
                            columns=self.getmetrics(key, db, sensor))

    def getchunker(self, key, db, sensor):
        """
        Returns the chunker object associated to the db name and sensor name

        Args
            key : string
                key to read values
            db : string
                db name
            sensor : string
                sensor name
        Return
            Chunker: Chunker object
        Raises
            Exception if db or sensor does not exist or not authorized
        """
        return Chunker(self._getmetadata(key, db, sensor, self.__FREQ_KEY))

    def getlasttimestamp(self, key, db, sensor):
        """Gets the last timestamp (in millis) of the data"""
        self.existsensor(key, db, sensor)
        self._check_auth(key, Actions.READ, db, sensor)
        chunks = self.FU.getchunks(db, sensor)
        if len(chunks) == 0:
            return 0
        dates, _ = self.read(key, db, sensor, float(chunks[-1]))
        return dates[-1]

    def read(self, key, db, sensor, start_ts=None, end_ts=None):
        """
        Reads data from the DB and returns it in either numpy arrays or in a pandas dataframe.
        All data is loaded in memory, so it could cause memory leaks if period is
        too long. In such cases, use read_iter to manage iterating by chunks

        For parameters and description, consult read_iter
        """
        values = None
        dates = None
        for new_dates, new_values, step in self.read_iter(key, db, sensor, start_ts,
                                                          end_ts):
            if values is None:
                values = new_values
                dates = new_dates
            else:
                # TODO: fix shapes to allow stack.Shape (5,) does not stack but (5,1) does
                values = np.vstack((values, new_values))
                dates = np.hstack((dates, new_dates))
        return dates, values

    def get_FU_path(self, *args):
        return self.FU.path(*args)

    def read_iter(self, key, db, sensor, start_ts=None, end_ts=None,
                  step=None):
        """
        Reads data from the DB and returns an iterator that gives data chunk by chunk
        as numpy arrays.

        Args
            key -- string
                Key to authorize read data
            db -- string
                Name of the db to read
            sensor -- string
                Name of the sensor to read
            start_ts -- timestamp (as produced with time.time())
                Starting date.(Default: start of the chunk that correspond to time.time())
            end_ts -- timestamp (as produced with time.time())
                with the ending date. (Default: time.time())
            step -- number
                Step (in seconds) for the data to read. (Default: None)

        Return
            (d, v, td) : tuple
                where d is a vector of timestamps, v a ndarray of values and td the tick duration
        Raises
            OSError
                if path is not valid
            ElementNotFoundException
                if sensor or db do not exist
            NotAuthorizedException
                if key is invalid to read data
        See also
            np2pd to convert numpy varray to pandas dataframe
        """

        self._check_auth(key, Actions.READ, db, sensor)
        self.existsensor(key, db, sensor)
        chunker = self.getchunker(key, db, sensor)
        step = step or chunker.chunk_duration
        SHAPE = chunker.np_shape(len(self.getmetrics(key, db, sensor)))

        # As a default, read current chunk (the one corresponding to current time
        start_ts = start_ts or chunker.chunk_timestamp(time.time())
        end_ts = end_ts or time.time()
        chunk = start_ts
        now = repr(time.time())

        class Cache(object):
            pass

        def cache_read(cache, file_name, start_t, end_ts, SHAPE, is_last_chunk, chunk_ts, tick_duration):
            new_dates, new_values = self._readchunk(file_name, start_t, end_ts,
                                                    SHAPE, is_last_chunk, chunk_ts,
                                                    tick_duration)
            cache.data_available = True
            cache.d = new_dates
            cache.v = new_values
            cache.fn = file_name

        if not hasattr(self, 'cache'):
            self.cache = dict()
        self.cache[now] = Cache()
        self.cache[now].data_available = False
        self.cache[now].fn = ""

        def get_chunk_filename(chunk):
            for compressed in (False, True):
                chunk_name = chunker.chunk_name(chunk, SHAPE[1], compressed=compressed)
                file_name = self.get_FU_path(db, sensor, chunk_name)
                if os.path.exists(file_name):
                    return file_name
            return file_name

        while True:
            chunk_ts = chunker.chunk_timestamp(chunk)
            is_last_chunk = chunk_ts == chunker.chunk_timestamp(end_ts)

            file_name = get_chunk_filename(chunk)
            # chunk_name = chunker.chunk_name(chunk, SHAPE[1])
            # file_name = self.get_FU_path(db, sensor, chunk_name)

            chunk += max(chunker.chunk_duration, step)
            next_file_name = get_chunk_filename(chunk)
            # next_file_name = self.get_FU_path(db, sensor, chunker.chunk_name(chunk, SHAPE[1]))
            # perform a cache of next chunk here
            cache = self.cache[now]
            if cache.data_available and cache.fn == file_name:
                new_dates, new_values = cache.d, cache.v
            else:
                new_dates, new_values = self._readchunk(file_name, start_ts, end_ts,
                                                        SHAPE, is_last_chunk, chunk_ts,
                                                        chunker.tick_duration)
            cache.data_available = False
            if not is_last_chunk:
                start_new_thread(cache_read, (cache, next_file_name, start_ts,
                                              end_ts, SHAPE, is_last_chunk, chunker.chunk_timestamp(chunk),
                                              chunker.tick_duration))

            #            o.toc("read chunk")
            if new_dates is not None:
                yield new_dates, new_values, chunker.tick_duration
            if chunker.chunk_timestamp(chunk) > end_ts:
                break
        del (self.cache[now])

    def _readchunk(self, file_name, start_ts, end_ts, SHAPE, is_last_chunk, chunk_ts: int, tick_duration: float):
        if os.path.isfile(file_name):
            #            o.tic()
            orig_chunk_value = self.FU.fast_read_np(file_name, SHAPE, dtype=DTYPE)
            if orig_chunk_value is None:
                return None, None
            positions = orig_chunk_value[:, 0].astype(np.float64)
            timestamps = (positions - 1) * tick_duration + chunk_ts
            # filter out empty values
            idx_filter = (timestamps >= start_ts) & (positions > 0)
            if is_last_chunk:
                idx_filter = idx_filter & (timestamps <= end_ts)
            chunk_value = orig_chunk_value[idx_filter, :]
            if chunk_value.shape[0] < len(orig_chunk_value[:, 0].nonzero()):
                print("oh oohhhhhhh")
            new_dates = timestamps[idx_filter]
            # Verify checksum
            if len(chunk_value) > 0:
                checksum_ok = np.isclose(chunk_value[:, 1:-1].sum(1), chunk_value[:, -1])
                if not checksum_ok.all():
                    invalids = (checksum_ok != 0).nonzero()[0]
                    chunk_value = chunk_value[checksum_ok, :]
                    new_dates = new_dates[checksum_ok]
                    # TODO: do logging
                    logger.warning("Found {} bad checksum(s) in chunk {}".format(len(invalids), file_name))
                    logger.warning("Invalid indexes: ")
                    logger.warning(invalids)
            new_values = chunk_value[:, range(1, SHAPE[1] - 1, 1)]
            # new_dates = chunk_value[:, 0].astype(np.float64) - 1 + chunk_ts
            # These copies forces contiguous data in the arrays
            # makes faster pickling buy might also make other things faster
            if not new_values.flags.c_contiguous:
                new_values = new_values.copy()
            if not new_dates.flags.c_contiguous:
                new_dates = new_dates.copy()
            return new_dates, new_values
        return None, None

    def get_mdf5(self, file_name):
        if os.path.isfile(file_name):
            return hashlib.md5(open(file_name, 'rb').read()).hexdigest()
        else:
            return 0


if __name__ == "__main__":
    db = OngTSDB(BASE_DIR)
    db.createdb(123, "hola")
