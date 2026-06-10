# -*- coding: utf-8 -*-
"""
Class to manage file storage (under BASE_DIR directory)
"""

import enum
import hashlib
import hmac
import os
import secrets
import shutil
import string
import threading
import time

import numpy as np
import pandas as pd
import ujson
from six.moves._thread import start_new_thread

from ong_tsdb import logger, LOCAL_TZ, BASE_DIR, DTYPE
from ong_tsdb.chunker import Chunker
from ong_tsdb.exceptions import (  # noqa: F401  -- re-exports for back-compat
    OngTSDBBaseException,
    NotAuthorizedException,
    ElementAlreadyExistsException,
    ElementNotFoundException,
    InvalidDataWriteException,
)
from ong_tsdb.fileutils import FileUtils, re_chunk_filename


class Actions(enum.Enum):
    CREATE = 1
    READ = 2
    WRITE = 3


class OngTSDB(object):
    __FREQ_KEY = "Freq"
    __METRICS_KEY = "Metrics"
    __METADATA_KEY = "Metadata"
    __READ_KEY = "Read_Key"
    __WRITE_KEY = "Write_key"

    # Class-level lock used only to serialize creation of per-sensor locks
    # (see _get_sensor_lock). It is NOT used to guard chunk I/O.
    _registry_lock = threading.Lock()

    def __init__(self, path=BASE_DIR):
        """Inits database in the path (defaults to BASE_DIR). There must be a CONFIG.JS in the path,
        otherwise it will be created with a new admin password that will be shown with logger.info"""
        # Per-sensor locks: serialise chunk I/O for the same (db, sensor)
        # without blocking work on other sensors. Created lazily.
        self._sensor_locks: dict = {}
        self.FU = FileUtils(path)
        if not os.path.isfile(self.FU.path_config()):
            # Bootstrap a new database at the requested path, NOT at the
            # global BASE_DIR. The previous implementation always wrote the
            # config to BASE_DIR, which was wrong when a custom path was
            # passed.
            os.makedirs(path, exist_ok=True)
            length = 20
            admin_key = "".join(
                secrets.choice(string.hexdigits) for _ in range(int(length))
            )
            with self.FU.safe_createfile(self.FU.path_config(), "w") as f:
                f.write(admin_key)
            # Print the key ONCE to stdout with a clear banner so the user
            # can copy it. Do NOT use logger.info here: persistent log files
            # would end up storing the key in cleartext. The path to the
            # config file is logged so it is recoverable later.
            print()
            print("=" * 60)
            print("  ong_tsdb: NEW DATABASE created at {}".format(path))
            print("  An admin key has been generated. Save it now:")
            print()
            print("    {}".format(admin_key))
            print()
            print("  Stored in: {}".format(self.FU.path_config()))
            print("  This message is NOT logged. Copy the key above.")
            print("=" * 60)
            print()
            logger.info("DB correctly setup at %s", path)
            logger.info(
                "Admin key generated (see %s); not logged for security",
                self.FU.path_config(),
            )

        with open(self.FU.path_config(), "r") as f:
            self.admin_key = f.readline().strip()
        self.db = dict()
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
                except Exception:
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
                        raise ElementAlreadyExistsException(
                            "Element already exists with different config"
                        )

    def __is_key(self, db, sensor, key_name, key_value) -> bool:
        """Checks if key_value correspond to a key_name from a sensor and database"""
        if db not in self.db:
            return False
        elif sensor not in self.db[db]:
            return False
        else:
            stored = self.db[db][sensor].get(key_name)
            if stored is None:
                return False
            return hmac.compare_digest(stored, key_value)

    def _get_sensor_lock(self, db: str, sensor: str) -> threading.Lock:
        """Returns the lock associated with (db, sensor), creating it on first use.

        The returned lock must be acquired by every method that touches the
        chunk files of that sensor (write_tick_numpy, add_new_metrics,
        _replace_chunk). It is safe to call this from any thread; the
        registry is itself protected by a class-level lock.
        """
        key = (db, sensor)
        with self._registry_lock:
            lock = self._sensor_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._sensor_locks[key] = lock
        return lock

    def _check_auth(self, key, action: "Actions", db, sensor):
        """Checks if key is valid for the action in sensor. Raises
        NotAuthorizedException otherwise. The exception message includes
        the action name and target (db/sensor) for easier debugging.
        """
        if hmac.compare_digest(key, self.admin_key):
            return  # admin key is valid for any action
        if action != Actions.CREATE:
            if self.__is_key(db, sensor, self.__WRITE_KEY, key):
                if action in (Actions.WRITE, Actions.READ):
                    return
            if self.__is_key(db, sensor, self.__READ_KEY, key):
                if action in (Actions.READ,):
                    return
        target = f"{db}/{sensor}" if sensor else (db or "<global>")
        raise NotAuthorizedException(f"Invalid key for {action.name} on {target}")

    def exist_db(self, key, db):
        """True id db exists"""
        return db in self.db

    def exist_sensor(self, key, db, sensor):
        """Checks if a sensor exists in database"""
        if db not in self.db or len(self.db[db].keys()) == 0:
            return (
                False  # Empty database, no sensor available and auth cannot be checked
            )
        self._check_auth(key, Actions.READ, db, sensor)
        if (
            not self.exist_db(key, db)
            or sensor not in self.db[db].keys()
            or not os.path.isdir(self.FU.path(db, sensor))
        ):
            return False
        return True

    def create_db(self, admin_key, db):
        self._check_auth(admin_key, Actions.CREATE, None, None)
        if self.exist_db(admin_key, db):
            raise ElementAlreadyExistsException(f"Database {db} already exists")
        self.__create_internal_structure(self.FU.path(db), self.db.keys())
        self.db[db] = dict()

    def delete_db(self, admin_key, db):
        self._check_auth(admin_key, Actions.CREATE, db, None)
        if self.exist_db(admin_key, db):
            shutil.rmtree(self.FU.path(db))
            if db in self.db:
                self.db.pop(db)
            return True
        else:
            return False

    def delete_sensor(self, admin_key, db, sensor):
        self._check_auth(admin_key, Actions.CREATE, db, sensor)
        if self.exist_sensor(admin_key, db, sensor):
            shutil.rmtree(self.FU.path(db, sensor))
            self.db[db].pop(sensor)
            return True
        else:
            return False

    def create_sensor(
        self,
        admin_key,
        db,
        sensor,
        period,
        write_key,
        read_key,
        metrics,
        force_update=False,
        metadata=None,
    ):
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
        :param metadata: Optional metadata to be included in the metrics (e.g. if metrics are a list, the names of the list)
        :return:
        """
        self._check_auth(admin_key, Actions.CREATE, db, sensor)
        if not self.exist_sensor(admin_key, db, sensor) or force_update:
            # Create only if does not exist
            _ = Chunker(period)  # Will raise exception if invalid period
            config = {
                self.__WRITE_KEY: write_key,
                self.__READ_KEY: read_key,
                self.__FREQ_KEY: period,
                self.__METRICS_KEY: metrics,
                self.__METADATA_KEY: metadata,
            }
            self.__create_internal_structure(
                self.FU.path(db, sensor), self.db[db].keys(), ujson.dumps(config)
            )
            self.db[db][sensor] = config
        else:
            raise ElementAlreadyExistsException(
                f"Sensor {sensor} already exist in {db}"
            )

    def update_metadata(self, key, db, sensor, new_metadata):
        """Updates metadata of an existing sensor"""
        self._check_auth(key, Actions.CREATE, db, sensor)
        if self.exist_sensor(key, db, sensor):
            self.db[db][sensor][self.__METADATA_KEY] = new_metadata
            self.__create_internal_structure(
                self.FU.path(db, sensor),
                self.db[db].keys(),
                ujson.dumps(self.db[db][sensor]),
            )

    def get_metrics(self, key, db, sensor, force_reload=False):
        """Returns the list of metrics of a certain db and sensor"""
        if force_reload:
            self.config_reload()
        return self._getmetadata(key, db, sensor, self.__METRICS_KEY)

    def get_metadata(self, key, db, sensor, force_reload=False):
        """Returns the list of metadata of a certain db and sensor"""
        if force_reload:
            self.config_reload()
        return self._getmetadata(key, db, sensor, self.__METADATA_KEY)

    def _getmetadata(self, key, db, sensor, field):
        return self.db[db][sensor].get(field)

    def get_numpy_row(self, key, db, sensor):
        """Returns an empty numpy row of the correct size for the sensor"""
        return np.zeros((1, len(self.get_metrics(key, db, sensor))))

    def __get_record_size(self, key, db, sensor):
        """Returns record size in bytes"""
        return np.dtype(DTYPE).itemsize * self.__get_array_size(key, db, sensor)

    def __get_array_size(self, key, db, sensor):
        """Returns record size in columns of the np.array writen in the chunks"""
        return 2 + len(self.get_metrics(key, db, sensor))

    def _replace_chunk(
        self,
        db,
        sensor,
        original_chunk_name,
        new_chunk_name,
        new_array=None,
        compressed=False,
    ):
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
            new_array = self.FU.fast_read_np(
                self.get_FU_path(db, sensor, original_chunk_name), dtype=DTYPE
            )
        with self.FU.safe_createfile(
            self.get_FU_path(db, sensor, new_chunk_name), "wb"
        ) as f:
            f.write(new_array.tobytes())
        os.remove(self.get_FU_path(db, sensor, original_chunk_name))

    def add_new_metrics(self, key, db, sensor, new_metrics: list, fill_value=0):
        """Adds new metric(s) to a sensor. To so, opens all chunks from the changing chunk ahead,
        adds empty column to all of them, changes the column name an deletes the old ones"""

        # Open all chunks after timestamp
        chunker = self.get_chunker(key, db, sensor)
        # start_ts = str(int(chunker.init_date(timestamp)))
        all_chunks = self.FU.getchunks(db, sensor)  # Names without path
        # chunks_to_change = [c for c in self.FU.getchunks(db, sensor) if c > start_ts]     # Names without path
        chunks_to_change = all_chunks
        metrics = self.get_metrics(key, db, sensor)
        period = self.db[db][sensor][self.__FREQ_KEY]
        write_key = self.db[db][sensor][self.__WRITE_KEY]
        read_key = self.db[db][sensor][self.__READ_KEY]
        self.create_sensor(
            self.admin_key,
            db,
            sensor,
            period,
            write_key,
            read_key,
            metrics + new_metrics,
            force_update=True,
        )
        self.config_reload()
        updated_metrics = self.get_metrics(key, db, sensor)
        # Lock the sensor for the whole rewrite: this both serialises against
        # concurrent write_tick_numpy calls and prevents a reader from
        # observing a half-rewritten chunk directory.
        with self._get_sensor_lock(db, sensor):
            for old_chunk_name in chunks_to_change:
                a = self.FU.fast_read_np(
                    self.get_FU_path(db, sensor, old_chunk_name), dtype=DTYPE
                )
                new_array = np.concatenate(
                    (
                        a[:, :-1],
                        np.full(
                            shape=(a.shape[0], len(new_metrics)),
                            dtype=a.dtype,
                            fill_value=fill_value,
                        ),
                        a[:, -1][:, None],
                    ),
                    axis=1,
                )
                parts = re_chunk_filename.fullmatch(old_chunk_name).groupdict()
                compressed = parts["compression"] is not None
                new_chunk_name = chunker.chunk_name(
                    int(parts["timestamp"]),
                    int(parts["n_columns"]) + len(new_metrics),
                    compressed,
                )
                self._replace_chunk(
                    db, sensor, old_chunk_name, new_chunk_name, new_array, compressed
                )

    def write_tick_numpy(
        self, key, db, sensor, np_values: np.array, np_timestamps=None
    ):
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
        self.exist_sensor(key, db, sensor)
        if np_values.shape[1] != len(self.get_metrics(key, db, sensor)):
            raise InvalidDataWriteException("Invalid number of cols of numpy array")
        # Give default value to timestamp if not provided.
        if np_timestamps is None:
            np_timestamps = np.full(np_values.shape[0], time.time(), dtype=np.float64)
        cols_chunk_array = self.__get_array_size(key, db, sensor)
        chunker = self.get_chunker(key, db, sensor)
        chunk_name = self.FU.path(
            db, sensor, chunker.chunk_name(np_timestamps[0], cols_chunk_array)
        )
        pos = chunker.getpos(np_timestamps)
        with self._get_sensor_lock(db, sensor):
            if not os.path.isfile(chunk_name):
                value_write = np.full(
                    (chunker.n_rows_per_chunk, cols_chunk_array),
                    fill_value=np.nan,
                    dtype=DTYPE,
                )
            else:
                with self.FU.get_open_func(chunk_name)(chunk_name, "rb") as f:
                    raw = f.read()
                expected_bytes = (
                    chunker.n_rows_per_chunk
                    * cols_chunk_array
                    * np.dtype(DTYPE).itemsize
                )
                if len(raw) != expected_bytes:
                    raise InvalidDataWriteException(
                        f"Refusing to overwrite corrupt chunk {chunk_name!r}: "
                        f"expected {expected_bytes} bytes, got {len(raw)}. "
                        f"Inspect manually before proceeding."
                    )
                value_write = np.frombuffer(raw, dtype=DTYPE)
                value_write.shape = (chunker.n_rows_per_chunk, cols_chunk_array)

            value_write = np.array(value_write)
            idx_not_nan = np.nonzero(~np.isnan(np_values))
            value_write[pos[idx_not_nan[0]], idx_not_nan[1] + 1] = np_values[
                idx_not_nan
            ]
            vw = value_write[pos, 1:-1]
            value_write[pos, -1] = np.nansum(vw, axis=1)
            value_write[pos, 0] = pos + 1
            with self.FU.safe_createfile(chunk_name, "wb") as f:
                f.write(value_write.tobytes())

    def np2pd(self, key, db, sensor, dates, values, tz=LOCAL_TZ):
        dateindex = pd.to_datetime(dates, unit="s", utc=True).tz_convert(tz)
        return pd.DataFrame(
            values, index=dateindex, columns=self.get_metrics(key, db, sensor)
        )

    def get_chunker(self, key, db, sensor):
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

    def get_last_timestamp(self, key, db, sensor):
        """Gets the last timestamp (in millis) of the data, None if no data available"""
        if not self.exist_sensor(key, db, sensor):
            return None
        self._check_auth(key, Actions.READ, db, sensor)
        chunks = self.FU.getchunks(db, sensor)
        if len(chunks) == 0:
            return None
        dates, _ = self.read(key, db, sensor, float(chunks[-1].split(".")[0]))
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
        for new_dates, new_values, step in self.read_iter(
            key, db, sensor, start_ts, end_ts
        ):
            if values is None:
                values = new_values
                dates = new_dates
            else:
                values = np.vstack((values, new_values))
                dates = np.hstack((dates, new_dates))
        return dates, values

    def get_FU_path(self, *args):
        return self.FU.path(*args)

    def read_iter(self, key, db, sensor, start_ts=None, end_ts=None, step=None):
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
        self.exist_sensor(key, db, sensor)
        chunker = self.get_chunker(key, db, sensor)
        step = step or chunker.chunk_duration
        SHAPE = chunker.np_shape(len(self.get_metrics(key, db, sensor)))

        # As a default, read current chunk (the one corresponding to current time)
        start_ts = start_ts or chunker.chunk_timestamp(time.time())
        # Truncate start_ts to the corresponding chunk duration. For example, if frequency = "1D" and start_ts is
        # the middle of the day, the start_ts is moved to the start of the day so data is read
        start_ts = chunker.tick_duration * (start_ts // chunker.tick_duration)
        end_ts = end_ts or time.time()
        chunk = start_ts

        class Cache(object):
            pass

        def cache_read(
            cache,
            file_name,
            start_t,
            end_ts,
            SHAPE,
            is_last_chunk,
            chunk_ts,
            tick_duration,
        ):
            # The main loop may have aborted by the time we run; in that case
            # just discard the result instead of writing to a freed cache.
            if not getattr(cache, "alive", False):
                return
            new_dates, new_values = self._read_chunk(
                file_name,
                start_t,
                end_ts,
                SHAPE,
                is_last_chunk,
                chunk_ts,
                tick_duration,
            )
            cache.data_available = True
            cache.d = new_dates
            cache.v = new_values
            cache.fn = file_name

        if not hasattr(self, "cache"):
            self.cache = dict()
        if not hasattr(self, "_cache_counter"):
            self._cache_counter = 0
        self._cache_counter += 1
        cache_key = self._cache_counter
        cache_obj = Cache()
        cache_obj.data_available = False
        cache_obj.fn = ""
        cache_obj.alive = True
        self.cache[cache_key] = cache_obj

        def get_chunk_filename(chunk):
            for compressed in (False, True):
                chunk_name = chunker.chunk_name(chunk, SHAPE[1], compressed=compressed)
                file_name = self.get_FU_path(db, sensor, chunk_name)
                if os.path.exists(file_name) and os.path.getsize(file_name) > 0:
                    return file_name
            return file_name

        try:
            while True:
                chunk_ts = chunker.chunk_timestamp(chunk)
                is_last_chunk = chunk_ts == chunker.chunk_timestamp(end_ts)

                file_name = get_chunk_filename(chunk)

                chunk += max(chunker.chunk_duration, step)
                next_file_name = get_chunk_filename(chunk)
                cache = self.cache[cache_key]
                if cache.data_available and cache.fn == file_name:
                    new_dates, new_values = cache.d, cache.v
                else:
                    new_dates, new_values = self._read_chunk(
                        file_name,
                        start_ts,
                        end_ts,
                        SHAPE,
                        is_last_chunk,
                        chunk_ts,
                        chunker.tick_duration,
                    )
                cache.data_available = False
                if not is_last_chunk:
                    start_new_thread(
                        cache_read,
                        (
                            cache,
                            next_file_name,
                            start_ts,
                            end_ts,
                            SHAPE,
                            is_last_chunk,
                            chunker.chunk_timestamp(chunk),
                            chunker.tick_duration,
                        ),
                    )

                if new_dates is not None:
                    yield new_dates, new_values, chunker.tick_duration
                if chunker.chunk_timestamp(chunk) > end_ts:
                    break
        finally:
            # Ensure the cache entry is removed even if the consumer breaks
            # out of the generator (e.g. with .close()) or an exception
            # propagates. The alive flag tells the background read thread
            # to drop its result if it has not started yet.
            cache_obj = self.cache.get(cache_key)
            if cache_obj is not None:
                cache_obj.alive = False
            self.cache.pop(cache_key, None)

    def _read_chunk(
        self,
        file_name,
        start_ts,
        end_ts,
        SHAPE,
        is_last_chunk,
        chunk_ts: int,
        tick_duration: float,
    ):
        if os.path.isfile(file_name):
            try:
                orig_chunk_value = self.FU.fast_read_np(file_name, SHAPE, dtype=DTYPE)
            except ValueError as e:
                # Corrupt chunk: skip it instead of taking down the whole
                # read. The user can run `python -m ong_tsdb verify` to
                # identify the offending file and decide whether to
                # delete it (so the next write regenerates it) or
                # restore it from backup.
                logger.error(
                    f"Skipping corrupt chunk {file_name!r}: {e}. "
                    f"Run `python -m ong_tsdb verify --db <db>` to inspect."
                )
                return None, None
            if orig_chunk_value is None:
                return None, None
            positions = orig_chunk_value[:, 0].astype(np.float64)
            timestamps = (positions - 1) * tick_duration + chunk_ts
            # filter out empty values
            idx_filter = (timestamps >= start_ts) & (positions > 0)
            if is_last_chunk:
                idx_filter = idx_filter & (timestamps <= end_ts)
            chunk_value = orig_chunk_value[idx_filter, :]
            n_nonzero = int((orig_chunk_value[:, 0] != 0).sum())
            if chunk_value.shape[0] < n_nonzero:
                logger.warning(
                    f"Filter dropped rows in {file_name!r}: had {n_nonzero} "
                    f"non-empty rows, returned {chunk_value.shape[0]}. "
                    f"Chunk integrity should be checked."
                )
            new_dates = timestamps[idx_filter]
            # Verify checksum
            if len(chunk_value) > 0:
                value_check = chunk_value[:, 1:-1]
                checksum_ok = np.isclose(
                    np.nansum(value_check, axis=1),
                    chunk_value[:, -1],
                )
                if not checksum_ok.all():
                    invalids = (checksum_ok != 0).nonzero()[0]
                    chunk_value = chunk_value[checksum_ok, :]
                    new_dates = new_dates[checksum_ok]
                    logger.warning(
                        "Found {} bad checksum(s) in chunk {}".format(
                            len(invalids), file_name
                        )
                    )
                    logger.warning("Invalid indexes: ")
                    logger.warning(invalids)
            new_values = chunk_value[:, range(1, SHAPE[1] - 1, 1)]
            if not new_values.flags.c_contiguous:
                new_values = new_values.copy()
            if not new_dates.flags.c_contiguous:
                new_dates = new_dates.copy()
            return new_dates, new_values
        return None, None

    def get_md5(self, file_name):
        if os.path.isfile(file_name):
            with open(file_name, "rb") as f:
                return hashlib.md5(f.read()).hexdigest()
        else:
            return 0
