import time
import zlib
import msgpack
import base64
import pandas as pd
import ujson
import urllib3
from ong_tsdb import config, logger, LOCAL_TZ, DTYPE
from ong_tsdb.database import OngTSDB
from urllib3.exceptions import MaxRetryError, TimeoutError, ConnectionError
from ong_utils.timers import OngTimer
import numpy as np

timer = OngTimer(False)


class OngTsdbClientBaseException(Exception):
    """Base Exception for the exceptions of this module"""


class NotAuthorizedException(OngTsdbClientBaseException):
    """Exception raised when 401 error is received from sever"""
    pass


class ServerDownException(OngTsdbClientBaseException):
    """Exception raised when cannot connect to server"""
    pass


class WrongAddressException(OngTsdbClientBaseException):
    """Raised when 404 error is received"""
    pass


class OngTsdbClient:

    def __init__(self, url:str, port, token:str):
        """
        Initializes client
        :param url: url of the ong_tsdb client. If empty or none, http://localhost will be used
        :param port: port of the ong_tsdb client
        :param token: the token to use for communication
        """
        self.server_url = url or "http://localhost"
        if self.server_url.endswith("/"):
            self.server_url = self.server_url[:-1]
        self.server_url += f":{port}"
        self.token = token
        self.headers = urllib3.make_headers(basic_auth=f'token:{self.token}')
        self.headers.update({"Content-Type": "application/json"})
        self.http = urllib3.PoolManager(retries=urllib3.Retry(total=20, connect=10,
                                                              backoff_factor=0.2))

    def _request(self, method, url, *args, **kwargs):
        """Execute request adding token to header. Raises Exception if unauthorized"""
        if 'headers' in kwargs:
            kwargs['headers'].update(self.headers)
        else:
            kwargs['headers'] = self.headers
        retval = None
        try:
            # retval = self.http.request(method, url, *args, **kwargs)
            retval = self.http.urlopen(method, url, *args, **kwargs)
        except OngTsdbClientBaseException as e:
            logger.exception(e)
            return None
        except (ConnectionError, MaxRetryError, TimeoutError):
            logger.error(f"Cannot connect to {url}")
            raise ServerDownException("Cannot connect to server, check if server is running")
        except Exception as e:
            logger.exception(e)
            logger.exception(f"Error reading {url}. Maybe server is down")
            return None
        # Check retval
        if retval:
            if retval.status == 401:
                raise NotAuthorizedException(f"Unauthorized, your token {self.token} is invalid for {url}")
            elif retval.status == 404:
                raise WrongAddressException(f"Error 404 in {url}")
        return retval

    def _put(self, *args, **kwargs) -> bool:
        """Executes a put request, returning true if success"""
        req = self._request("put", *args, **kwargs)
        if req:
            if req.status == 201:
                return True
            else:
                logger.debug(f"{req.status} {req.data}")
                return False
        return False

    def _post_retval(self, *args, **kwargs):
        """Executes a post and returns a tuple formed by a boolean with the success of the query
        and dict with the json of the response (or None if success=False)"""
        timer.tic("Executing post")
        req = self._request("post", *args, **kwargs)
        timer.toc("Executing post")
        if req:
            if req.status in (200, 201):
                return True, ujson.loads(req.data)
            else:
                logger.info(f"{req.status} {req.data.decode()}")
                return False, None

    def _post(self, *args, **kwargs) -> bool:
        """Executes a put request, returning true if success. If gzip=False data is not sent gzipped"""
        do_gzip = kwargs.pop("gzip", True)
        if "body" in kwargs:
            body = kwargs['body']
            if do_gzip and len(body) > 1024:
                kwargs['headers'] = {'content-encoding': 'gzip'}
                timer.tic("Gzipping body")
                kwargs['body'] = zlib.compress(body)
                timer.toc("Gzipping body")
        success, json = self._post_retval(*args, **kwargs)
        return success

    def create_db(self, database) -> bool:
        """Creates a new db. Returns true if success"""
        return self._post(self._make_url("/db/") + database)

    def create_sensor(self, database, sensor, period, metrics, read_key, write_key) -> bool:
        """
        Creates a sensor in a database
        :param database: database name
        :param sensor: new sensor name
        :param period: string with a number and a code of frequency (e.g. "1s", "5m", "1h", "1D")
        :param metrics: list of measurements in this sensor
        :param read_key: key for reading from this sensor
        :param write_key: key for writing in this sensor
        :return: True on success
        """
        data = dict(period=period, metrics=metrics, write_key=write_key, read_key=read_key)
        return self._post(self._make_url(f"/db/{database}/sensor/{sensor}"), body=ujson.dumps(data).encode())

    def write(self, sequence: list) -> bool:
        """Writes data to database, using influx format, e.g. a list of strings with the following format:
        "{dabatase},{ignored_key}={sensor} {metrics} {ts}"
        Also sequence can be a list of tuples of database, sensor, metrics, ts
        ts is the timestamp in nanoseconds
        """
        timer.tic("total post execution")
        if sequence:
            if isinstance(sequence[0], str):
                return self._post(self._make_url("/influx"), body="\n".join(sequence).encode())
            elif isinstance(sequence[0], (list, tuple)):
                timer.tic("Using msgpack")
                body = msgpack.dumps(sequence)
                timer.toc("Using msgpack")
                retval = self._post(self._make_url("/influx_binary"), body=body, gzip=False)
                timer.toc("total post execution")
                return retval
            else:
                return False
        else:
            return False

    def write_df(self, db: str, sensor: str, df) -> bool:
        """Writes a pandas dataframe into a certain database and sensor.
        Pandas data frame must be indexed by dates and have metrics/measurements as columns"""
        # Check index
        pass
        # Generate a sequence out of the given data
        sequence = []
        for idx, row in df.iterrows():
            sequence.append((db, sensor, list(row.index), list(row.values), idx.value))

        return self.write(sequence)


    def config_reload(self):
        """Forces a config reload of server (e.g. for manually modifying sensors)"""
        return self._post(self._make_url("/config_reload"))

    def _make_url(self, url):
        """Retunrs url for queries"""
        return self.server_url + url

    def get_lasttimestamp(self, db, sensor):
        """Returns last timestamp (millis) of data stored for a sensor in a db"""
        success, json = self._post_retval(self._make_url(f"/{db}/{sensor}/last_timestamp"))
        if success:
            return json['last_timestamp']
        else:
            return None

    def get_metrics(self, db, sensor):
        """Returns list of metrics of a sensor"""
        success, json = self._post_retval(self._make_url(f"/{db}/{sensor}/search"))
        return json if success else None

    def read_grafana(self, db, sensor, date_from, date_to=None, metrics=None) -> pd.DataFrame:
        """
        Reads data from db and returns it as a pandas dataframe, using grafana endpoints.
        This is much slower than read, so it should not be used
        :param db: name of db
        :param sensor: name of sensor
        :param date_from: date (datetime alike object) from which data will be read
        :param date_to: date (datetime alike object) up to which data will be read
        (optional, now would be used if not given)
        :param metrics: list of metrics to read (all metrics if not given)
        :return: a pandas dataframe
        """
        # Creates a post query with grafana style
        metrics = metrics or self.get_metrics(db, sensor)
        date_to = date_to or pd.Timestamp.now()
        data = {
            "range": {
                "from": date_from.isoformat(),
                "to": date_to.isoformat()
            },
            "targets": [dict(target=t) for t in metrics],
        }
        success, data = self._post_retval(self._make_url(f"/{db}/{sensor}/query"), body=ujson.dumps(data).encode())
        if not success:
            return None
        targets = [d['target'] for d in data]
        dp_idx = [[pd.Timestamp.utcfromtimestamp(d1[1] / 1e3).tz_localize("UTC").astimezone(LOCAL_TZ) for d1 in
                   d['datapoints']] for d in data]
        dp_val = [[d1[0] for d1 in d['datapoints']] for d in data]
        df = pd.DataFrame(np.array(dp_val).T, columns=targets, index=dp_idx[0])
        return df

    def local_read(self, db, sensor, date_from, date_to=None, metrics=None) -> pd.DataFrame:
        """
        Reads data from db and returns it as a pandas dataframe. Reads it from a local database not using server,
        so it won't work if database is not hosted in localhost
        :param db: name of db
        :param sensor: name of sensor
        :param date_from: date (datetime alike object) from which data will be read
        :param date_to: date (datetime alike object) up to which data will be read
        (optional, now would be used if not given)
        :param metrics: list of metrics to read (all metrics if not given)
        :return: a pandas dataframe
        """
        _db = OngTSDB()
        end_ts = date_to.timestamp() if date_to else None
        data = _db.read(self.token, db, sensor, start_ts=date_from.timestamp(), end_ts=end_ts)
        df = _db.np2pd(self.token, db, sensor, data[0], data[1])
        if metrics:
            df = df.loc[:, metrics]
        return df

    def read(self, db, sensor, date_from, date_to=None, metrics=None) -> pd.DataFrame:
        """
        Reads data from db and returns it as a pandas dataframe. Reads it from a local database not using server,
        so it won't work if database is not hosted in localhost
        :param db: name of db
        :param sensor: name of sensor
        :param date_from: date (datetime alike object) from which data will be read
        :param date_to: date (datetime alike object) up to which data will be read
        (optional, now would be used if not given)
        :param metrics: list of metrics to read (all metrics if not given)
        :return: a pandas dataframe
        """
        #_db = OngTSDB()
        end_ts = date_to.timestamp() if date_to else None
        #metrics = ",".join(metrics) if metrics else None

        body = ujson.dumps(dict(
            start_ts=date_from.timestamp(),
            end_ts=end_ts,
        ))

        # resp = self._request("post", self._make_url(f"/{db}/{sensor}/read_df"), body=body)
        # success = resp is not None
        # if not success:
        #     return None
        #
        # metrics_db = self.get_metrics(db, sensor)
        # dates_len = int(len(resp.data) / (len(metrics_db) + 2) / np.dtype(DTYPE).itemsize * np.dtype(np.float64).itemsize)
        # dates = np.frombuffer(resp.data[:dates_len])
        # values = np.frombuffer(resp.data[dates_len:], dtype=DTYPE)

        # resp = self._request("post", self._make_url(f"/{db}/{sensor}/read_df"), body=body)
        # metrics_db = self.get_metrics(db, sensor)
        # bts = base64.decodebytes(resp.data)
        # dates_len = int(
        #     len(bts) / (len(metrics_db) + 2) / np.dtype(DTYPE).itemsize * np.dtype(np.float64).itemsize)
        # # len of dates is the key of the json, value contains concatenated bytes of dates and values
        # # bts = base64.decodebytes(js_resp[str(dates_len)].encode())
        # dates = np.frombuffer(bts[:dates_len])
        # values = np.frombuffer(bts[dates_len:], dtype=DTYPE)

        success, js_resp = self._post_retval(self._make_url(f"/{db}/{sensor}/read_df"), body=body)
        # len of dates is the key of the json, value contains concatenated bytes of dates and values
        if not success:
            return None
        metrics_db = js_resp.pop("metrics").split(",")
        dates_len = int(next(iter(js_resp.keys())))
        bts = base64.decodebytes(js_resp[str(dates_len)].encode())
        dates = np.frombuffer(bts[:dates_len])
        values = np.frombuffer(bts[dates_len:], dtype=DTYPE)
        # metrics_db = self.get_metrics(db, sensor)

        values.shape = len(dates), int(values.shape[0] / len(dates))
        dateindex = pd.to_datetime(dates, unit='s', utc=True).tz_convert(LOCAL_TZ)
        df = pd.DataFrame(values, index=dateindex, columns=metrics_db)
        if metrics is not None:
            df = df.loc[:, metrics]
        return df


if __name__ == '__main__':
    client = OngTsdbClient(url=config('url'), port=config('port'), token=config('admin_token'))
    print(client.create_db("ejemplo"))
    print(client.create_sensor("ejemplo", "sensor1", "1s", ["active", "reactive"], write_key=config('write_token'),
                               read_key=config('read_token')))
    client = OngTsdbClient(url=config('url'), port=config('port'), token=config('write_token'))
    while True:
        ts = time.time_ns()
        client.write([f"ejemplo,circuit=sensor1 active=9,reactive=10 {ts}",
                      f"ejemplo,circuit=sensor1 active=11 {ts}",
                      f"ejemplo,circuit=sensor1 reactive=12 {ts}",
                      f"ejemplo,circuit=sensor1 reactive=13,active=14 {ts}",
                      f"ejemplo,circuit=sensor1 reactive=15,active=16,nueva=17 {ts}",
                      ])
        time.sleep(1)

