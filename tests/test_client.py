import time
from unittest import TestCase, main

import pandas as pd
import numpy as np

from ong_tsdb import config, LOCAL_TZ
from ong_tsdb.client import OngTsdbClient
from ong_tsdb.database import OngTSDB
from ong_utils import OngTimer

DB_TEST = "testing_database"
timer = OngTimer()


def get_sensor_name(freq):
    return f"testing_sensor_{freq}"


class TestOngTsdbClient(TestCase):
    _db = OngTSDB()
    admin_client = OngTsdbClient(url=config('url'), token=config('admin_token'))
    read_client = OngTsdbClient(url=config('url'), token=config('read_token'))
    write_client = OngTsdbClient(url=config('url'), token=config('write_token'))
    admin_key = config("admin_token")
    write_key = config("write_token")
    read_key = config("read_token")
    sensor_freqs = "1s", "1h", "1d", "15m"

    def setUp(self) -> None:
        """Deletes previous versions of databases and creates a new structure for TEST database"""
        # if self._db.exist_db(self.admin_key, DB_TEST):
        #     self._db.delete_db(self.admin_key, DB_TEST)
        if self.admin_client.exist_db(DB_TEST):
            self.admin_client.delete_db(DB_TEST)
        print(self.admin_client.config_reload())
        print(self.admin_client.create_db(DB_TEST))
        for freq in self.sensor_freqs:
            sensor_name = get_sensor_name(freq)
            if self.admin_client.exist_sensor(DB_TEST, sensor_name):
                self.admin_client.delete_sensor(DB_TEST, sensor_name)
            # if self._db.exist_sensor(self.admin_key, DB_TEST, sensor_name):
            #    self._db.delete_sensor(config("admin_token"), DB_TEST, sensor_name)
            sensor_created_ok = self.admin_client.create_sensor(DB_TEST, sensor_name, freq,
                                                                ["active", "reactive"], write_key=self.write_key,
                                                                read_key=self.read_key)
            self.assertIsNone(self.admin_client.get_metadata(DB_TEST, sensor_name), "Metadata is not None")
            print(f"Sensor {sensor_name} created {sensor_created_ok}")
        print("Test database created")

    def write_ts(self, n_periods, data_freq, sensor_name):

        now = pd.Timestamp.now(tz=LOCAL_TZ).replace(microsecond=0)
        if data_freq.endswith("h"):
            now = now.replace(second=0, minute=0)
        if data_freq.endswith("d"):
            now = now.replace(hour=0, second=0, minute=0)

        timestamps = pd.date_range(now, periods=n_periods, freq=data_freq)

        sequence = ["{DB_TEST},circuit={sensor_name} active=9,reactive=10 {ts}",
                    "{DB_TEST},circuit={sensor_name} active=11 {ts}",
                    "{DB_TEST},circuit={sensor_name} reactive=12 {ts}",
                    "{DB_TEST},circuit={sensor_name} reactive=13,active=14 {ts}",
                    "{DB_TEST},circuit={sensor_name} reactive=15,active=16,nueva=17 {ts}",
                    "{DB_TEST},circuit={sensor_name} reactive=18,nueva=19 {ts}",
                    ]

        value_to_write = []
        for idx, t in enumerate(timestamps):
            ts = t.value
            self.assertGreaterEqual(time.time(), t.timestamp(), "Writing future data!!!")
            i = idx % len(sequence)
            value_to_write.append(sequence[i].format(DB_TEST=DB_TEST, sensor_name=sensor_name, ts=ts))

        success = self.write_client.write(value_to_write)
        self.assertTrue(success, f"Error writing: {value_to_write}")
        print(f"Written OK: {value_to_write[:1000]}")
        self._db.config_reload()
        self._db.FU.verify_all_chunks(filter_db_name=DB_TEST, print_per_chunk_data=len(timestamps) < 100)
        start_ts = min(timestamps).timestamp()
        timer.tic("Reading directly from db")
        data = self._db.read(self.read_key, DB_TEST, sensor_name, start_ts=start_ts)
        df = self._db.np2pd(self.read_key, DB_TEST, sensor_name, data[0], data[1])
        timer.toc("Reading directly from db")
        timer.tic("Reading from local_read")
        df_local = self.read_client.local_read(DB_TEST, sensor_name, min(timestamps))
        timer.toc("Reading from local_read")
        self.assertEqual(len(df_local.index), len(df.index), "Local client index is not correct")
        self.assertTrue(df.equals(df_local), "Local client df values are not correct")
        print(df)
        for idx, (df_ts, ts) in enumerate(zip(df.index, reversed(timestamps))):
            # if df_ts.timestamp() != ts.timestamp():
            self.assertEqual(df_ts.timestamp(), ts.timestamp(),
                             f"Written data ts {ts} does not correspond to read data ts {df_ts} in {idx=}")
        self.assertEqual(len(df.index), len(timestamps), "Length of the data read does not match the written data")

        self.assertEqual(len(timestamps), df.shape[0], "Not all ticks have been read")
        # read using client
        db_metrics = self.read_client.get_metrics(DB_TEST, sensor_name)
        db_metrics = None

        # Several methods for reading data to check speeds and proper functioning
        timer.tic("Reading from read")
        df_client = self.read_client.read(DB_TEST, sensor_name, min(timestamps), metrics=db_metrics)
        timer.toc("Reading from read")
        self.assertEqual(len(df_client.index), len(df.index), "Client index length is not correct")
        self.assertTrue(df.equals(df_local), "Client df values are not correct")

        # Checks for last timestamp stored in database. Should not raise any exception
        for client in self.read_client, self.admin_client, self.read_client:
            last_ts = client.get_lasttimestamp(DB_TEST, sensor_name)
            last_date = client.get_lastdate(DB_TEST, sensor_name)
            last_date_local = client.get_lastdate(DB_TEST, sensor_name, LOCAL_TZ)
            print(f"{client.token=} {sensor_name=} {last_ts=} {last_date=} {last_date_local=}")
        pass

    def test_write_data1h_sensor1h(self):
        """Test writing in database each 1 second"""
        sensor_name = get_sensor_name("1h")
        self.write_ts(n_periods=10, data_freq="-1h", sensor_name=sensor_name)

    def test_write_data200d_sensor1h(self):
        """Test writing in database each 1 second"""
        self.write_ts(n_periods=10, data_freq="-200d", sensor_name=get_sensor_name("1h"))

    def test_write_data1d_sensor1h(self):
        """Test writing in database each 1 second"""
        self.write_ts(n_periods=10, data_freq="-1d", sensor_name=get_sensor_name("1h"))

    def test_write_data1s_sensor1s(self):
        """Test writing in database each 1 second"""
        self.write_ts(n_periods=10, data_freq="-1s", sensor_name=get_sensor_name("1s"))

    def test_write_data1d_sensor1s(self):
        """Tests writing in database each 1 day"""
        self.write_ts(n_periods=10, data_freq="-1d", sensor_name=get_sensor_name("1s"))

    def test_read_data1d_sensor1s(self):
        """Tests writing 10k data and read it to see different speeds"""
        # self.write_ts(n_periods=12, data_freq="-80min", sensor_name=get_sensor_name("1s"))
        # self.write_ts(n_periods=40, data_freq="-350min", sensor_name=get_sensor_name("1s"))
        # self.write_ts(n_periods=50, data_freq="-250min", sensor_name=get_sensor_name("1s"))
        self.write_ts(n_periods=10000, data_freq="-10min", sensor_name=get_sensor_name("1s"))

    def test_write_df(self):
        """Test writing data as a pandas dataframe"""
        n_points = 10
        metrics = ['una', 'dos', 'tres']
        freq = "15m"
        self.assertIn(freq, self.sensor_freqs, "Invalid frequency")
        freq_pandas = "15T"
        start_t = pd.Timestamp(2021, 10, 10).tz_localize(LOCAL_TZ)
        df = pd.DataFrame(np.ones((n_points, len(metrics))), columns=metrics,
                          index=pd.date_range(start=start_t, periods=n_points, freq="-" + freq_pandas)).sort_index()
        success = self.write_client.write_df(DB_TEST, get_sensor_name(freq), df)
        self.assertTrue(success, "Error writing data")
        df2 = self.read_client.read(DB_TEST, get_sensor_name(freq), df.index.min(), metrics=df.columns)
        self.assertSequenceEqual(df.shape, df2.shape, "Data writen and data read don't have the same shape")
        self.assertSequenceEqual(list(df.index), list(df2.index), "Data writen and data read don't have the same index")
        self.assertTrue((df == df2).all().all(), "Data writen and data read are not the same")

    def test_metadata(self):
        """Checks for writing and reading list metrics and metadata as list"""
        level_names = ['one', 'two', 'three']
        metrics = [['A', 'B', 'C'], ['D', 'E', 'F']]
        metadata_sensor = "metadata_sensor"
        self.admin_client.create_sensor(DB_TEST, metadata_sensor, "1D", metrics, read_key=self.read_key,
                                        write_key=self.write_key, level_names=level_names)
        metrics_db = self.read_client.get_metrics(DB_TEST, metadata_sensor)
        metadata_db = self.read_client.get_metadata(DB_TEST, metadata_sensor)
        self.assertEqual(metrics, metrics_db, "Incorrect metrics")
        self.assertEqual(dict(level_names=level_names), metadata_db, "Incorrect metadata")
        # Same test with different types of timestamps
        for now in (
                pd.Timestamp.utcnow().normalize().tz_convert(LOCAL_TZ),     # Date in LOCAL_TZ
                pd.Timestamp.now().normalize(),                             # Naive date
                pd.Timestamp.utcnow().normalize(),                          # Naive UTC date
                ):
            df_write = pd.DataFrame([[1.0, 2.0]], columns=pd.MultiIndex.from_tuples(metrics,
                                                                                    names=level_names), index=[now])
            self.write_client.write_df(DB_TEST, metadata_sensor, df_write)
            df_read = self.read_client.read(DB_TEST, metadata_sensor, now)
            df_read = df_read.astype(df_write.dtypes)
            print(df_read)
            self.assertTrue(df_read.equals(df_write), f"Dataframes do not match for {now=}")
        # Now test a bad timestamp
        now_local_tz = pd.Timestamp.now(tz=LOCAL_TZ).normalize()
        df_read = self.read_client.read(DB_TEST, metadata_sensor, now_local_tz)
        self.assertFalse(df_read.empty, "Read an empty dataframe!")

        # Test trying to change metadata
        new_level_names = ['X', 'Y', 'Z']
        self.assertTrue(self.admin_client.set_level_names(DB_TEST, metadata_sensor, new_level_names))
        new_df_read = self.read_client.read(DB_TEST, metadata_sensor, now)
        self.assertSequenceEqual(new_df_read.columns.names, new_level_names, "Level_names did not change!")
        pass

    def test_exists(self):
        """Test exist_sensor and exist_database"""
        good_sensor_name = get_sensor_name(self.sensor_freqs[0])
        self.assertTrue(self.read_client.exist_sensor(DB_TEST, good_sensor_name),
                        "Sensor does not exit!")
        self.assertFalse(self.read_client.exist_sensor(DB_TEST, good_sensor_name + "_random_junk"),
                         "Sensor should not exit!")

    def test_delete(self):
        """Test delete_database and delete_sensor, creating a fake database and sensor"""
        db_delete = DB_TEST + "_junk"
        sensor_delete = "sensor1"
        self.assertFalse(self.read_client.exist_db(db_delete), "Delete database already existed!")
        self.assertTrue(self.admin_client.create_db(db_delete))
        self.assertTrue(self.admin_client.create_sensor(db_delete, sensor_delete, self.sensor_freqs[0], list(),
                                                        self.read_key, self.write_key))
        self.assertTrue(self.read_client.exist_sensor(db_delete, sensor_delete), "Sensor delete was not created!")
        self.assertFalse(self.read_client.delete_sensor(db_delete, sensor_delete), "Read client deleted a sensor!")
        self.assertFalse(self.write_client.delete_sensor(db_delete, sensor_delete), "Write client deleted a sensor!")
        self.assertTrue(self.admin_client.delete_sensor(db_delete, sensor_delete),
                        "Admin client could not delete a sensor!")
        self.assertFalse(self.read_client.exist_sensor(db_delete, sensor_delete), "Sensor was not deleted!")
        self.assertFalse(self.read_client.delete_db(db_delete), "Read client deleted a database!")
        self.assertFalse(self.write_client.delete_db(db_delete), "Write client deleted a database!")
        self.assertTrue(self.admin_client.delete_db(db_delete), "Admin client could not delete a database!")
        self.assertFalse(self.read_client.exist_db(db_delete), "Database was not deleted!")

    def test_last_timestamp(self):
        """Test last timestamp functionality"""
        last_ts = self.admin_client.get_lasttimestamp(DB_TEST, get_sensor_name(self.sensor_freqs[0]))
        last_ts = self.read_client.get_lasttimestamp(DB_TEST, get_sensor_name(self.sensor_freqs[0]))
        last_ts = self.write_client.get_lasttimestamp(DB_TEST, get_sensor_name(self.sensor_freqs[0]))
        print(f"{last_ts=}")


    def tearDown(self) -> None:
        """Deletes Test Database"""
        if self.admin_client.exist_db(DB_TEST):
            if self.admin_client.delete_db(DB_TEST):
                print("Test database deleted")
            else:
                print("Test database could not be deleted")
        else:
            print("Test database not existed")


if __name__ == '__main__':
    main()
