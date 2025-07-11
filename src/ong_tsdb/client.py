import base64
import time
import zlib

import msgpack
import numpy as np
import pandas as pd
import ujson
import urllib3
from ong_utils import OngTimer, create_pool_manager, get_cookies, cookies2header
from urllib3.exceptions import MaxRetryError, TimeoutError, ConnectionError

from ong_tsdb import config, logger, LOCAL_TZ, DTYPE, HTTP_COMPRESS_THRESHOLD
from ong_tsdb.check_versions import check_version_and_raise
from ong_tsdb.database import OngTSDB
from ong_tsdb.exceptions import OngTsdbClientBaseException, NotAuthorizedException, ProxyNotAuthorizedException, \
    ServerDownException, WrongAddressException

timer = OngTimer(False)


class OngTsdbClient:

    def __init__(self, url: str, token: str, retry_total=20, retry_connect=None, retry_backoff_factor=.2,
                 proxy_auth_body: dict = None, validate_server_version: bool = True, **kwargs):
        """
        Initializes client. It needs just an url (that includes port, parameter port is kept for backward compatibility
         but it is not used anymore) and a token.
         Example: admin_client = OngTsdbClient(url=config('url'), token=config('admin_token'))
        :param url: url of the ong_tsdb client. If empty or none, http://localhost:5000 will be used. Param
        port is not used anymore
        :param token: the token to use for communication
        :param retry_total: param total for urllib3.Retry. Defaults to 20
        :param retry_connect: param connect for urllib3.Retry. Defaults to 10 if host is not localhost else 1
        :param retry_backoff_factor: param backoff_factor for urllib3.Retry. Defaults to 0.2
        :param proxy_auth_body: and optional dict with the body to be sent to the auth proxy (if ongtsdb is behind a
        server with authentication, this is the body to be posted to the login page)
        :param validate_server_version: True (default) to validate that the version of the server is greater or equal
         than the same as the client to raise a WrongVersionException, False otherwise
        """
        self.validate_server_version = validate_server_version
        if proxy_auth_body is None:
            proxy_auth_body = dict()
        self.proxy_auth_body = proxy_auth_body
        self.server_url = url or "http://localhost:5000"
        if self.server_url.startswith("http://localhost"):
            retry_connect = retry_connect or 1
        else:
            retry_connect = retry_connect or 10
        if self.server_url.endswith("/"):
            self.server_url = self.server_url[:-1]
        self.token = token
        self.headers = {"Content-Type": "application/json"}
        self.update_token(self.token)
        self.http = create_pool_manager(total=retry_total, connect=retry_connect, backoff_factor=retry_backoff_factor)
        # Force reload configuration, that also serves as a connection test to make sure server is running
        for attempt in range(2):
            try:
                res = self.config_reload()
                break
            except NotAuthorizedException:
                pass  # Not important
                break
            except ProxyNotAuthorizedException as pnae:
                """Needs to send proxy authentication. 
                It will only be sent if server responds with application/json
                Response body will be compose of form field received from server (if any) updated with
                the proxy_auth_body dictionary"""
                if pnae.response.headers.get("content-type").startswith("application/json"):
                    js_resp = ujson.loads(pnae.response.data)
                    url = js_resp.get("url")
                    body = js_resp.get("form", dict())
                    body.update(self.proxy_auth_body)
                    # body = ujson.dumps(self.proxy_auth_body)
                    cookies = get_cookies(pnae.response)
                    headers = dict(**self.headers, **cookies2header(cookies))
                    res = self.http.request("POST", self._make_url(url), headers=headers, body=ujson.dumps(body))
                    if res.data and res.headers.get("content-type").startswith("application/json") \
                            and ujson.loads(res.data).get("http_code") == 200:
                        cookies = get_cookies(res)
                        self.headers.update(cookies2header(cookies))
                    else:
                        logger.debug(f"Could not log in, response = {res.data}")
                        raise pnae
                    continue
                else:
                    logger.debug(f"Proxy auth response not understood, needs a json with form and url fields. "
                                 f"Received {pnae.response.data}")
                    raise pnae
            except (ServerDownException, WrongAddressException):
                break

    def update_token(self, token):
        """Allows changing token of an already created client"""
        self.token = token
        self.headers.update(urllib3.make_headers(basic_auth=f'token:{self.token}'))

    def _request(self, method, url, *args, **kwargs):
        """Execute request adding token to header. Raises Exception if unauthorized.
        :param method: get, post, delete...
        :param url: full url
        :param headers: optional argument for additional headers to add to default headers
        :param *args: optional arguments for urlopen
        :param **kwargs: optional arguments for urlopen
        """
        if 'headers' in kwargs:
            kwargs['headers'].update(self.headers)
        else:
            kwargs['headers'] = self.headers
        retval = None
        try:
            retval = self.http.request(method, url, *args, **kwargs)
            # retval = self.http.urlopen(method, url, *args, **kwargs)
        except OngTsdbClientBaseException as e:
            logger.exception(e)
            return None
        except (ConnectionError, MaxRetryError, TimeoutError):
            logger.error(f"Cannot connect to {url}")
            raise ServerDownException(f"Cannot connect to server in address={url}, check if server is running or server"
                                      f" url={self.server_url} used in constructor is correct and includes port ")
        except Exception as e:
            logger.exception(e)
            logger.exception(f"Error reading {url}. Maybe server is down")
            return None
        # Check retval
        if retval:
            if retval.status == 401:
                if "json" in retval.headers.get("Content-Type"):
                    js_res = ujson.loads(retval.data)
                    if js_res.get("http_code") == 407:
                        raise ProxyNotAuthorizedException(
                            f"Unauthorized, you need to set up a user and password for the proxy",
                            response=retval)
                raise NotAuthorizedException(f"Unauthorized, your token '{self.token}' is invalid for {url}")
            if retval.status == 407:
                raise ProxyNotAuthorizedException(f"Unauthorized, you need to set up a user and password for the proxy")
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
                retval = ujson.loads(req.data)
                if isinstance(retval, dict):
                    if self.validate_server_version:
                        server_version = retval.get("version")
                        check_version_and_raise(server_version)
                return True, retval
            else:
                logger.info(f"{req.status} {req.data.decode()}")
                return False, None

    def _post(self, *args, **kwargs) -> bool:
        """Executes a put request, returning true if success. If gzip=False data is not sent gzipped"""
        do_gzip = kwargs.pop("gzip", True)
        if "body" in kwargs:
            body = kwargs['body']
            if do_gzip and len(body) > HTTP_COMPRESS_THRESHOLD:
                kwargs['headers'] = {'content-encoding': 'gzip'}
                timer.tic("Gzipping body")
                kwargs['body'] = zlib.compress(body)
                timer.toc("Gzipping body")
        success, json = self._post_retval(*args, **kwargs)
        return success

    def exist_db(self, database: str) -> bool:
        """Returns True if database exists"""
        try:
            res = self._request("get", self._make_url(f"/db/{database}"))
            return True
        except:
            return False

    def create_db(self, database) -> bool:
        """Creates a new db. Returns true if success"""
        return self._post(self._make_url("/db/") + database)

    def delete_db(self, database: str) -> bool:
        """Returns True if database could be deleted"""
        try:
            res = self._request("delete", self._make_url(f"/db/{database}"))
            return True
        except:
            return False

    def exist_sensor(self, database: str, sensor: str) -> bool:
        """Returns True if sensor exists"""
        try:
            res = self._request("get", self._make_url(f"/db/{database}/sensor/{sensor}"))
            return True
        except:
            return False

    def create_sensor(self, database, sensor, period, metrics, read_key, write_key, metadata=None,
                      level_names=None) -> bool:
        """
        Creates a sensor in a database
        :param database: database name
        :param sensor: new sensor name
        :param period: string with a number and a code of frequency (e.g. "1s", "5m", "1h", "1D")
        :param metrics: list of measurements in this sensor
        :param read_key: key for reading from this sensor
        :param write_key: key for writing in this sensor
        :param metadata: optional dict with metadata (json serializable) to include in the database. If metrics is
        a list of lists, then the key "level_names" of this dictionary will be used to form the multiindex
        :param level_names: optional list with level names, that will stored in metadadata as metadata['level_names']
        :return: True on success
        """
        if metadata is not None and not isinstance(metadata, dict):
            raise ValueError(f"Wrong metadata type, it must be a dict. Passed metadata={metadata}")
        if level_names:
            metadata = metadata or dict()
            metadata['level_names'] = level_names
        data = dict(period=period, metrics=metrics, write_key=write_key, read_key=read_key, metadata=metadata)
        return self._post(self._make_url(f"/db/{database}/sensor/{sensor}"), body=ujson.dumps(data).encode())

    def delete_sensor(self, database: str, sensor: str) -> bool:
        """Returns True if sensor could be deleted"""
        try:
            res = self._request("delete", self._make_url(f"/db/{database}/sensor/{sensor}"))
            return True
        except:
            return False

    def write(self, sequence: list, fill_value=0) -> bool:
        """Writes data to database, using influx format, e.g. a list of strings with the following format:
        "{dabatase},{ignored_key}={sensor} {metrics} {ts}"
        Also sequence can be a list of tuples of database, sensor, metrics, ts
        ts is the timestamp in nanoseconds
        Fill_value is the default value when adding a new metric
        """
        if fill_value == 0:
            # 0 is the default value, there is no need for sending it
            fill_value = ""
        else:
            fill_value = f"/{fill_value}"

        timer.tic("total post execution")
        if sequence:
            if isinstance(sequence[0], str):
                return self._post(self._make_url(f"/influx{fill_value}"), body="\n".join(sequence).encode())
            elif isinstance(sequence[0], (list, tuple)):
                timer.tic("Using msgpack")
                body = msgpack.dumps(sequence)
                timer.toc("Using msgpack")

                retval = self._post(self._make_url(f"/influx_binary{fill_value}"), body=body, gzip=False)
                timer.toc("total post execution")
                return retval
            else:
                return False
        else:
            return False

    def write_df(self, db: str, sensor: str, df, fill_value=0) -> bool:
        """Writes a pandas dataframe into a certain database and sensor.
        Pandas data frame must be indexed by dates and have metrics/measurements as columns"""
        # Check index
        pass
        # Generate a sequence out of the given data
        sequence = []
        for idx, row in df.iterrows():
            sequence.append((db, sensor, list(row.index), list(row.values), idx.value))

        return self.write(sequence, fill_value=fill_value)

    def config_reload(self):
        """Forces a config reload of server (e.g. for manually modifying sensors)"""
        return self._post(self._make_url("/config_reload"))

    def _make_url(self, url):
        """Returns url for queries"""
        return self.server_url + url

    def get_lasttimestamp(self, db, sensor):
        """Returns last timestamp (millis) of data stored for a sensor in a db"""
        success, json = self._post_retval(self._make_url(f"/{db}/{sensor}/last_timestamp"))
        if success:
            return json['last_timestamp']
        else:
            return None

    def get_lastdate(self, db, sensor, tz=None):
        """Returns last date of data stored for a sensor in a db. If tz=None, then a naive date is returned, else
        a date converted to the selected TZ is returned"""
        ts = self.get_lasttimestamp(db, sensor)
        if ts is None:
            return None
        else:
            utc_date = pd.Timestamp.utcfromtimestamp(ts)
            if tz is None:
                return utc_date
            else:
                return utc_date.tz_convert("UTC").astimezone(tz)

    def get_metrics(self, db, sensor):
        """Returns list of metrics of a sensor"""
        success, json = self._post_retval(self._make_url(f"/{db}/{sensor}/metrics"))
        return json if success else None

    def get_metadata(self, db, sensor):
        """Returns metadata of a sensor"""
        success, json = self._post_retval(self._make_url(f"/{db}/{sensor}/metadata"))
        if not json:
            return None
        metadata = json.get("metadata", json)  # For retro compatibility
        return metadata if success else None

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
        date_to = date_to or pd.Timestamp.utcnow()
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

    def set_level_names(self, db, sensor, level_names):
        """
        Sets level_names for a sensor (must exist previously)
        """
        metadata = self.get_metadata(db, sensor) or dict()
        metadata['level_names'] = level_names
        res = self._post(self._make_url(f"/db/{db}/sensor/{sensor}/set_metadata"), body=ujson.dumps(metadata))
        return res

    def read(self, db, sensor, date_from: pd.Timestamp, date_to: pd.Timestamp = None,
             metrics: list = None) -> pd.DataFrame:
        """
        Reads data from db and returns it as a pandas dataframe.
        Index is converted to the same TZ as date_from (if no TZ then naive dates are returned)
        :param db: name of db
        :param sensor: name of sensor
        :param date_from: date (datetime alike object) from which data will be read
        :param date_to: date (datetime alike object) up to which data will be read
        (optional, now would be used if not given)
        :param metrics: list of metrics to read (all metrics if not given)
        :return: a pandas dataframe
        """
        # _db = OngTSDB()
        end_ts = date_to.timestamp() if date_to else None
        # metrics = ",".join(metrics) if metrics else None

        body = ujson.dumps(dict(
            start_ts=date_from.timestamp(),
            end_ts=end_ts,
        ))

        success, js_resp = self._post_retval(self._make_url(f"/{db}/{sensor}/read_df"), body=body,
                                             headers={"Content-Encoding": "gzip"})
        # len of dates is the key of the json, value contains concatenated bytes of dates and values
        if not success:
            return None
        metrics_db = js_resp.pop("metrics")
        metadata_db = js_resp.pop("metadata")
        if metrics_db and isinstance(metrics_db[0], (list, tuple)):
            level_names = metadata_db.get("level_names") if metadata_db else None
            metrics_db = pd.MultiIndex.from_tuples(metrics_db, names=level_names)
        dates_len = int(next(iter(js_resp.keys())))
        # Processes answer taking into account compression
        resp_data = js_resp[str(dates_len)].encode("ISO-8859-1")
        if js_resp.get("compressed", False):
            resp_data = zlib.decompress(resp_data)
        bts = base64.decodebytes(resp_data)

        dates = np.frombuffer(bts[:dates_len])
        values = np.frombuffer(bts[dates_len:], dtype=DTYPE)
        # metrics_db = self.get_metrics(db, sensor)
        if date_from.tz:
            dateindex = pd.to_datetime(dates, unit='s', utc=True).tz_convert(date_from.tz)
        else:
            dateindex = pd.to_datetime(dates, unit='s')
        if len(values) > 0:
            values.shape = len(dates), int(values.shape[0] / len(dates))
        else:
            values = None
        df = pd.DataFrame(values, index=dateindex, columns=metrics_db)
        if metrics is not None:
            df = df.loc[:, metrics]
        return df

    def __del__(self):
        """Forces http pool manager to be cleared (to avoid warning in unittest)"""
        self.http.clear()


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
