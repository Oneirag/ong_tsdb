import time
from unittest import TestCase

import pandas as pd

from ong_tsdb import config, LOCAL_TZ
from ong_tsdb.client import OngTsdbClient
from ong_tsdb.code.database import OngTSDB
from ong_utils import OngTimer

DB_TEST = "testing_database"
timer = OngTimer()


def get_sensor_name(freq):
    return f"testing_sensor_{freq}"


class TestOngTsdbClient(TestCase):
    _db = OngTSDB()
    port = config('test_port', config('port'))
    admin_client = OngTsdbClient(url=config('url'), port=port, token=config('admin_token'))
    read_client = OngTsdbClient(url=config('url'), port=port, token=config('read_token'))
    write_client = OngTsdbClient(url=config('url'), port=port, token=config('write_token'))
    admin_key = config("admin_token")
    write_key = config("write_token")
    read_key = config("read_token")
    sensor_freqs = "1s", "1h", "1d"

    def setUp(self) -> None:
        """Deletes previous versions of databases and creates a new structure for TEST database"""
        if self._db.existdb(self.admin_key, DB_TEST):
            self._db.deletedb(self.admin_key, DB_TEST)
        print(self.admin_client.config_reload())
        print(self.admin_client.create_db(DB_TEST))
        for freq in self.sensor_freqs:
            sensor_name = get_sensor_name(freq)
            if self._db.existsensor(self.admin_key, DB_TEST, sensor_name):
                self._db.deletesensor(config("admin_token"), DB_TEST, sensor_name)
            sensor_created_ok = self.admin_client.create_sensor(DB_TEST, sensor_name, freq,
                                                                ["active", "reactive"], write_key=self.write_key,
                                                                read_key=self.read_key)
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
        self.assertTrue((df == df_local).all().all(), "Local client df values are not correct")
        print(df)
        self.assertEqual(len(df.index), len(timestamps), "Length of the data read does not match the written data")
        for idx, (df_ts, ts) in enumerate(zip(df.index, reversed(timestamps))):
            # if df_ts.timestamp() != ts.timestamp():
            self.assertEqual(df_ts.timestamp(), ts.timestamp(),
                             f"Written data ts {ts} does not correspond to read data ts {df_ts} in {idx=}")

        self.assertEqual(len(timestamps), df.shape[0], "Not all ticks have been read")
        # read using client
        db_metrics = self.read_client.get_metrics(DB_TEST, sensor_name)
        db_metrics = None
        timer.tic("Reading from read")
        df_client = self.read_client.read(DB_TEST, sensor_name, min(timestamps), metrics=db_metrics)
        timer.toc("Reading from read")
        self.assertEqual(len(df_client.index), len(df.index), "Client index length is not correct")
        self.assertTrue((df == df_client).all().all(), "Client df values are not correct")
        pass

    def test_write_data1h_sensor1h(self):
        """Test writing in database each 1 second"""
        self.write_ts(n_periods=10, data_freq="-1h", sensor_name=get_sensor_name("1h"))

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
        self.write_ts(n_periods=10000, data_freq="-10min", sensor_name=get_sensor_name("1s"))

    def tearDown(self) -> None:
        """Deletes Test Database"""
        if self._db.existdb(self.admin_key, DB_TEST):
            self._db.deletedb(self.admin_key, DB_TEST)
        print("Test database deleted")
