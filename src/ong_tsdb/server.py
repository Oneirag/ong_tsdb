import logging
import sys

import ujson

if sys.gettrace() is None:
    from gevent import monkey

    monkey.patch_all()

import zlib
from functools import wraps
from base64 import encodebytes

import msgpack
import numpy as np
import pandas as pd
from flask import Flask, jsonify, request, stream_with_context
from gevent.pywsgi import WSGIServer
from werkzeug.exceptions import HTTPException, Unauthorized

from ong_tsdb import config, DTYPE, HELLO_MSG, HTTP_COMPRESS_THRESHOLD, logger, __version__
from ong_tsdb.database import OngTSDB, NotAuthorizedException
from ong_tsdb.server_utils import split_influx
from ong_utils import OngTimer, is_debugging, find_available_port

time_it = OngTimer(enabled=is_debugging(), logger=logger,
                   log_level=logging.DEBUG if not is_debugging() else logging.INFO)
_db = OngTSDB()

app = Flask(__name__)


@app.errorhandler(HTTPException)
def handle_http_exception(e):
    """Return JSON instead of HTML for HTTP errors."""
    # now you're handling non-HTTP exceptions only
    return make_js_response("HTTP Exception found", e.code,
                            code=e.code,
                            name=e.name,
                            description=e.description,
                            )


@app.errorhandler(Exception)
def handle_exception(e):
    # pass through HTTP errors
    if isinstance(e, HTTPException):
        return e

    # now you're handling non-HTTP exceptions only
    return make_js_response("Generic Exception found", 500,
                            code=500,
                            name=e.__class__.__name__,
                            description=str(e),
                            )


def make_js_response(msg, http_code=200, **kwargs):
    """Returns a js with msg as field and the http code"""
    return jsonify(msg=msg, http_code=http_code, ok=http_code == 200, version=__version__, **kwargs), http_code


def auth_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth:
            raise Unauthorized("Authorization header needed")
        key = auth['password']
        try:
            retval = f(*args, **kwargs, key=key)
            return retval
        except NotAuthorizedException as nae:
            raise Unauthorized(str(nae))

    return decorated


@app.errorhandler(404)
def resource_not_found(e):
    return make_js_response("Page not found", 404, error=str(e))


@app.route('/config_reload', methods=["POST"])
@auth_required
def config_reload(key):
    """Reloads configuration just in case any external change happened. Currently, no token is required"""
    _db.config_reload()
    return make_js_response("Configuration refreshed OK")


@app.route("/")
def hello():
    """Says hello to check that server is running"""
    return make_js_response(HELLO_MSG, 200)


@app.route('/db/<database>', methods=["POST"])
@auth_required
def create_db(database, key):
    """Creates a new database (returns 406 if database already existed) """
    if _db.exist_db(key, database):
        return make_js_response(f"Database {database} already exists", 406)
    else:
        _db.create_db(key, database)
        return make_js_response(f"Database {database} created ok", 201)


@app.route('/db/<database>', methods=["DELETE"])
@auth_required
def delete_db(database, key):
    """Deletes a database (returns 404 if database not existed)"""
    if _db.exist_db(key, database):
        _db.delete_db(key, database)
        return make_js_response(f"Database {database} deleted", 200)
    else:
        return make_js_response(f"Database {database} was not found", 404)


@app.route('/db/<database>', methods=["GET"])
@auth_required
def exists_db(database, key):
    """Checks if database exists, returning 200 if exists and 404 otherwise"""
    if _db.exist_db(key, database):
        return make_js_response(f"Database {database} exists", 200)
    else:
        return make_js_response(f"Database {database} does not exist", 404)


@app.route('/db/<database>/sensor/<sensor>', methods=["POST"])
@auth_required
def create_sensor(database, sensor, key):
    """Creates a new sensor in database (returns 406 if sensor already existed) """
    if _db.exist_sensor(key, database, sensor):
        return make_js_response(f"Sensor {sensor} already exists in Database {database}", 406)
    else:
        _db.create_sensor(key, database, sensor, **request.json)
        return make_js_response(f"Sensor {sensor} created ok in database {database} ", 201)


@app.route('/db/<database>/sensor/<sensor>', methods=["DELETE"])
@auth_required
def delete_sensor(database, sensor, key):
    """Deletes a sensor in database (returns 404 if sensor did not exist) """
    if _db.exist_sensor(key, database, sensor):
        _db.delete_sensor(key, database, sensor)
        return make_js_response(f"Sensor {sensor} in Database {database} deleted", 200)
    else:
        return make_js_response(f"Sensor {sensor} or Database {database} not found", 404)


@app.route('/db/<database>/sensor/<sensor>', methods=["GET"])
@auth_required
def exists_sensor(database, sensor, key):
    """Check if sensor exists in database, return 200 if exist and returns 404 otherwise"""
    if _db.exist_sensor(key, database, sensor):
        return make_js_response(f"Sensor {sensor} and database {database} exist")
    else:
        return make_js_response(f"Sensor {sensor} or database {database} do not exists", 404)


@app.route('/db/<database>/sensor/<sensor>/set_metadata', methods=["POST"])
@auth_required
def set_metadata(database, sensor, key):
    """Sets metadata for a sensor, returning 503 exception if something was wrong"""
    if _db.exist_sensor(key, database, sensor):
        new_metadata = ujson.loads(request.data)
        _db.update_metadata(key, database, sensor, new_metadata)
        return make_js_response(f"sensor={sensor} in database={database} configuration changed ok", 200)
    else:
        return make_js_response(f"sensor={sensor} did not exist in database={database} ", 404)


def write_point_list(key: str, point_list: list, fill_value: float = 0) -> None:
    """
    Writes data from point_list into database
    :param key: token for writing
    :param point_list: list of db, sensor, metrics (list of metric,sensor) and timestamp
    :param fill_value: when a new metrics is created, this is the value for the non existing data.
    Defaults to 0
    :return:
    """
    db_meter_data_dict = dict()

    class DbMeterData:
        def __init__(self, key, db, sensor):
            self.db = db
            self.sensor = sensor
            self.chunker = _db.get_chunker(key, db, sensor)
            self.db_metrics = _db.get_metrics(key, db, sensor)
            self.new_metrics = list()
            self.timestamps = dict()
            self.data_metrics = dict()
            self.data_values = dict()

        @property
        def dict_key(self):
            return self.db, self.sensor

        def append_data(self, timestamp, data_metrics, data_values):
            """For a ts, data_metrics is a list of strings with the metrics and data_values
            is the list of numerical values corresponding to data_metrics
            """
            timestamp = timestamp / 1e9
            init_date = int(self.chunker.chunk_timestamp(timestamp))
            if init_date not in self.timestamps:
                self.timestamps[init_date] = list()
                self.data_metrics[init_date] = list()
                self.data_values[init_date] = list()
            self.timestamps[init_date].append(timestamp)
            self.data_metrics[init_date].append(data_metrics)
            self.data_values[init_date].append(data_values)
            self.new_metrics.extend([m for m in data_metrics
                                     if m not in self.db_metrics and m not in self.new_metrics])

    for db, sensor, data_metrics, data_values, timestamp in point_list:
        if (db, sensor) not in db_meter_data_dict:
            db_meter_data_dict[(db, sensor)] = DbMeterData(key, db, sensor)
        db_meter_data = db_meter_data_dict[db, sensor]
        db_meter_data.append_data(timestamp, data_metrics, data_values)

    # For each group, send a numpy array to write_ticks_numpy so they are written all together
    for db_meter_data in db_meter_data_dict.values():
        if db_meter_data.new_metrics:
            _db.add_new_metrics(key, db_meter_data.db, db_meter_data.sensor, db_meter_data.new_metrics,
                                fill_value=fill_value)
        metrics_db = _db.get_metrics(key, db_meter_data.db, db_meter_data.sensor)
        for (chunk_name, data_metrics), data_values, metrics_ts in zip(db_meter_data.data_metrics.items(),
                                                                       db_meter_data.data_values.values(),
                                                                       db_meter_data.timestamps.values()):
            np_values = np.full((len(data_metrics), len(metrics_db)), np.nan, dtype=DTYPE)
            for idx, (dt_metrics, dt_values) in enumerate(zip(data_metrics, data_values)):
                np_values[idx, [metrics_db.index(m) for m in dt_metrics]] = dt_values
            np_ts = np.array(metrics_ts)
            _db.write_tick_numpy(key, db_meter_data.db, db_meter_data.sensor, np_values, np_ts)


def parse_fill_value(fill_value) -> float:
    try:
        fill_value = float(fill_value)
    except:
        fill_value = 0
    return fill_value


@app.route('/influx', methods=["POST"], defaults={'fill_value': 0})
@app.route('/influx/<fill_value>', methods=["POST"])
@auth_required
def write_point(key, fill_value):
    data = request.data
    if request.headers.get('Content-Encoding', "") == "gzip":
        data = zlib.decompress(data)
    try:
        # First parse input data
        line_points = [split_influx(line.decode()) for line in data.splitlines()]
        write_point_list(key, line_points, fill_value=parse_fill_value(fill_value))
    except Exception as e:
        # TODO: Inform about num_line and line in the error description
        raise e
    return make_js_response(f"{len(line_points)} lines inserted ok")


@app.route('/influx_binary', methods=["POST"], defaults={'fill_value': 0})
@app.route('/influx_binary/<fill_value>', methods=["POST"])
@auth_required
def write_point_bin(key, fill_value):
    """Fill value is the value to fill chunks when no old value is found"""
    data = request.data
    if request.headers.get('Content-Encoding', "") == "gzip":
        data = zlib.decompress(data)
    data = msgpack.loads(data)
    try:
        # First parse input data
        write_point_list(key, data, fill_value=parse_fill_value(fill_value))
    except Exception as e:
        # TODO: Inform about num_line and line in the error description
        raise e
    return make_js_response(f"{len(data)} lines inserted ok")


@app.route("/<db_name>/<sensor_name>/last_timestamp", methods=["POST"])
@auth_required
def get_lasttimestamp(db_name, sensor_name, key):
    """Returns a json with the last timestamp in the key 'last_timestamp'"""
    return make_js_response(msg=None, last_timestamp=_db.get_last_timestamp(key, db_name, sensor_name))


@app.route("/<db_name>/<sensor_name>/read_df", methods=["post"])
@auth_required
def read_df(db_name, sensor_name, key=None):
    """
    Read data in a dataframe. Receives a json payload with "start_ts" and "end_ts" as keys
    :param db_name: name of db
    :param sensor_name: name of sensor
    :param key: token for reading
    :return: a dataframe turned into bytes
    """
    payload = request.json
    start_ts = payload['start_ts']
    end_ts = payload.get('end_ts', None)

    with time_it.context_manager("Reading data"):
        dates, values = _db.read(key, db_name, sensor_name, start_ts=start_ts, end_ts=end_ts)

    if dates is not None:
        with time_it.context_manager("Converting to bytes"):
            bytes_dates = dates.tobytes()
            bytes_values = values.tobytes()
            encoded_numpy = encodebytes(bytes_dates + bytes_values)  # .decode()
        with time_it.context_manager("Reading metrics and metadata"):
            metrics = _db.get_metrics(key, db_name, sensor_name)
            metadata = _db.get_metadata(key, db_name, sensor_name)
        # return encoded_numpy and the list of metrics
        # if more than 1024 data, compress JUST DATA to send it faster if client headers asked for it
        compressed = len(bytes_dates) > HTTP_COMPRESS_THRESHOLD and request.headers.get("content-encoding",
                                                                                        "") == "gzip"
        with time_it.context_manager("Compressing"):
            if compressed:
                encoded_numpy = zlib.compress(encoded_numpy)
        key_data = str(len(bytes_dates))
        retval = {
            key_data: encoded_numpy.decode("ISO-8859-1"),
            "metrics": metrics,
            "metadata": metadata,
            "compressed": compressed,
            "version": __version__
        }
        return retval
    else:
        return make_js_response("No data", 404)


@app.route("/<db_name>/<sensor_name>/metadata", methods=["POST"])
@auth_required
def get_metadata(db_name, sensor_name, key=""):
    """Returns a JSON with the metadata of the db and sensor"""
    metadata = _db.get_metadata(key, db_name, sensor_name)
    return make_js_response(msg=None, metadata=metadata)


#########################################
#   Grafana endpoints
#########################################
@app.route("/<db_name>/<sensor_name>")
@auth_required
def grafana_index(db_name, sensor_name, key=None):
    """This endpoint is called by grafana to make sure JSON input data works"""
    return jsonify(dict(db=db_name, sensor=sensor_name, key=key))


@app.route("/<db_name>/<sensor_name>/query", methods=["POST"])
@auth_required
def grafana_query_chunked(db_name, sensor_name, key=""):
    """Reads data and returns it streamed. Receives a post request with the data that has to read"""
    logger.debug(f"Received grafana query: {request.json}")

    def grafana_query(db_name, sensor_name, key):
        """The query itself, that yields the response and will be used later to create streamed response"""
        datetotimestamp = lambda x: pd.Timestamp(x).timestamp()
        start_t = datetotimestamp(request.json['range']['from'])
        end_t = datetotimestamp(request.json['range']['to'])
        targets = [t['target'] for t in request.json['targets']]
        max_Datapoints = request.json.get('maxDataPoints')
        # print(f"{start_t=}, {end_t=}, {targets=}, {max_Datapoints=}")
        metrics = _db.get_metrics(key, db_name, sensor_name, force_reload=True)
        res = dict()
        for t in targets:
            res[t] = list()
        tick_time_spread = None if max_Datapoints is None else (end_t - start_t + 1) / float(max_Datapoints)
        # print(f"{tick_time_spread=}")
        n_data_read = 0
        for dates, values, tick_duration in _db.read_iter(key,
                                                          db_name, sensor_name,
                                                          start_t, end_t, step=tick_time_spread):
            # For very long time queries some chunks can be discarded
            if len(dates) > 0 and dates[-1] >= start_t:
                for i in range(len(dates)):
                    dt = dates[i]
                    if dt >= start_t:
                        for t in targets:
                            if not pd.isna(values[i, metrics.index(t)]):
                                res[t].append("[%f,%f]" % (values[i, metrics.index(t)], dt * 1000))
                        while start_t < dt:
                            start_t += tick_time_spread or tick_duration
                        n_data_read += 1
                        if start_t > dates[-1]:
                            break
        yield '['
        for i in range(len(targets)):
            t = targets[i]
            if i > 0:
                yield ","
            yield '{{"target":"{target}","datapoints":['.format(target=t)
            yield ','.join(res[t])
            yield ']}'
        yield ']'

    # Returns stream response
    return app.response_class(stream_with_context(grafana_query(db_name, sensor_name, key)),
                              mimetype="application/json")


@app.route("/<db_name>/<sensor_name>/metrics", methods=["POST"])
@app.route("/<db_name>/<sensor_name>/search", methods=["POST"])  # for Grafana
@auth_required
def grafana_search(db_name, sensor_name, key=""):
    """Returns a JSON with the metrics of the db and sensor"""
    return jsonify(_db.get_metrics(key, db_name, sensor_name))


@app.route("/get_md5/<filename>")
def grafana_get_md5(filename):
    return jsonify(_db.get_mdf5(filename))


def main():
    if sys.gettrace() is None:
        # No debug mode
        host = config('host')
        port = find_available_port(config('port'), logger=logger)
        http_server = WSGIServer((host, port), app)
        http_server.serve_forever()
    else:
        # Debug mode, using test port and test host if available (otherwise host and port)
        host = config('test_host', config('host'))
        port = find_available_port(config('test_port', config('port')), logger=logger)
        # app.run(host=host, port=port, debug=True)
        app.run(host=host, port=port)


if __name__ == '__main__':
    main()
