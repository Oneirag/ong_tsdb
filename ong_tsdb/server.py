import sys

if sys.gettrace() is None:
    from gevent import monkey

    monkey.patch_all()

import zlib
from functools import wraps
import io
from base64 import encodebytes

import msgpack
import numpy as np
import pandas as pd
from flask import Flask, jsonify, request, stream_with_context, send_file
from gevent.pywsgi import WSGIServer
from werkzeug.exceptions import HTTPException, Unauthorized

from ong_tsdb import config, DTYPE
from ong_tsdb.code.database import OngTSDB, NotAuthorizedException
from ong_tsdb.code.server_utils import split_influx

_db = OngTSDB()

app = Flask(__name__)


@app.errorhandler(HTTPException)
def handle_http_exception(e):
    """Return JSON instead of HTML for HTTP errors."""
    return jsonify({
        "code": e.code,
        "name": e.name,
        "description": e.description,
    }), e.code


@app.errorhandler(Exception)
def handle_exception(e):
    # pass through HTTP errors
    if isinstance(e, HTTPException):
        return e

    # now you're handling non-HTTP exceptions only
    return jsonify({
        "code": 500,
        "name": e.__class__.__name__,
        "description": str(e),
    }), 500


def make_js_response(msg, http_code=200):
    """Returns a js with msg as field and the http code"""
    return jsonify(msg=msg), http_code


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
    return jsonify(error=str(e)), 404


@app.route('/config_reload', methods=["POST"])
@auth_required
def config_reload(key):
    """Reloads configuration just in case any external change happened. Currently no token is required"""
    _db.config_reload()
    return make_js_response("Configuration refreshed OK")


@app.route('/db/<database>', methods=["POST"])
@auth_required
def create_db(database, key):
    """Creates a new database (returns 406 if database already existed) """
    if _db.existdb(key, database):
        return make_js_response(f"Database {database} already exists", 406)
    else:
        _db.createdb(key, database)
        return make_js_response(f"Database {database} created ok", 201)


@app.route('/db/<database>/sensor/<sensor>', methods=["POST"])
@auth_required
def create_sensor(database, sensor, key):
    """Creates a new sensor in database (returns 406 if sensor already existed) """
    if _db.existsensor(key, database, sensor):
        return make_js_response(f"Sensor {sensor} already exists in Database {database}", 406)
    else:
        _db.createsensor(key, database, sensor, **request.json)
        return make_js_response(f"Sensor {sensor} created ok in database {database} ", 201)


def write_point_list(key: str, point_list: list) -> None:
    """
    Writes data from point_list into database
    :param key: token for writing
    :param point_list: list of db, sensor, metrics (list of metric,sensor) and timestamp
    :return:
    """
    db_meter_data_dict = dict()

    class DbMeterData:
        def __init__(self, key, db, sensor):
            self.db = db
            self.sensor = sensor
            self.chunker = _db.getchunker(key, db, sensor)
            self.db_metrics = _db.getmetrics(key, db, sensor)
            self.new_metrics = list()
            self.timestamps = dict()
            self.data_metrics = dict()
            self.data_values = dict()

        @property
        def dict_key(self):
            return self.db, self.sensor

        def append_data(self, timestamp, data_metrics, data_values):
            """For a ts, data_metrics is a list of strings with the metrics and data_values
            is the list of numerical values corresponding to data_metrics"""
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

    # For each group, send a numpy array to writeticks_numpy so they are written all together
    for db_meter_data in db_meter_data_dict.values():
        if db_meter_data.new_metrics:
            _db.add_new_metrics(key, db_meter_data.db, db_meter_data.sensor, db_meter_data.new_metrics)
        metrics_db = _db.getmetrics(key, db_meter_data.db, db_meter_data.sensor)
        for (chunk_name, data_metrics), data_values, metrics_ts in zip(db_meter_data.data_metrics.items(),
                                                                       db_meter_data.data_values.values(),
                                                                       db_meter_data.timestamps.values()):
            np_values = np.full((len(data_metrics), len(metrics_db)), np.nan, dtype=DTYPE)
            for idx, (dt_metrics, dt_values) in enumerate(zip(data_metrics, data_values)):
                np_values[idx, [metrics_db.index(m) for m in dt_metrics]] = dt_values
            np_ts = np.array(metrics_ts)
            _db.writetick_numpy(key, db, sensor, np_values, np_ts)


@app.route('/influx', methods=["POST"])
@auth_required
def write_point(key):
    data = request.data
    if request.headers.get('Content-Encoding', "") == "gzip":
        data = zlib.decompress(data)
    try:
        # First parse input data
        line_points = [split_influx(line.decode()) for line in data.splitlines()]
        write_point_list(key, line_points)
    except Exception as e:
        # TODO: Inform about num_line and line in the error description
        raise e
    return make_js_response(f"{len(line_points)} lines inserted ok")


@app.route('/influx_binary', methods=["POST"])
@auth_required
def write_point_bin(key):
    data = request.data
    if request.headers.get('Content-Encoding', "") == "gzip":
        data = zlib.decompress(data)
    data = msgpack.loads(data)
    try:
        # First parse input data
        write_point_list(key, data)
    except Exception as e:
        # TODO: Inform about num_line and line in the error description
        raise e
    return make_js_response(f"{len(data)} lines inserted ok")


@app.route("/<db_name>/<sensor_name>/last_timestamp", methods=["POST"])
@auth_required
def get_lasttimestamp(db_name, sensor_name, key):
    """Returns a json with the last timestamp in the key 'last_timestamp'"""
    return jsonify(dict(last_timestamp=_db.getlasttimestamp(key, db_name, sensor_name)))


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

    dates, values = _db.read(key, db_name, sensor_name, start_ts=start_ts, end_ts=end_ts)

    if dates is not None:

        # buff = io.BytesIO()
        # buff.write(dates.tobytes())
        # buff.write(values.tobytes())
        # buff.seek(0)
        # return send_file(buff, download_name=str(len(dates)))
        bytes_dates = dates.tobytes()
        bytes_values = values.tobytes()
        encoded_numpy = encodebytes(bytes_dates + bytes_values) #.decode()
        metrics = _db.getmetrics(key, db_name, sensor_name)
        # return encoded_numpy and the list of metrics
        retval = {
            str(len(bytes_dates)): encoded_numpy.decode(),
            "metrics": ",".join(metrics)
        }
        return retval
    else:
        return make_js_response("No data", 404)


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

    def grafana_query(db_name, sensor_name, key):
        """The query itself, that yields the response and will be used later to create streamed response"""
        datetotimestamp = lambda x: pd.Timestamp(x).timestamp()
        start_t = datetotimestamp(request.json['range']['from'])
        end_t = datetotimestamp(request.json['range']['to'])
        targets = [t['target'] for t in request.json['targets']]
        max_Datapoints = request.json.get('maxDataPoints')
        # print(f"{start_t=}, {end_t=}, {targets=}, {max_Datapoints=}")
        metrics = _db.getmetrics(key, db_name, sensor_name, force_reload=True)
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


@app.route("/<db_name>/<sensor_name>/search", methods=["POST"])
@auth_required
def grafana_search(db_name, sensor_name, key=""):
    """Returns a JSON with the metrics of the db and sensor"""
    return jsonify(_db.getmetrics(key, db_name, sensor_name))


@app.route("/get_md5/<filename>")
def grafana_get_md5(filename):
    return jsonify(_db.get_mdf5(filename))


if __name__ == '__main__':
    if sys.gettrace() is None:
        # No debug mode
        http_server = WSGIServer((config('host'), config('port')), app)
        http_server.serve_forever()
    else:
        # Debug mode, using test port and test host if available (otherwise host and port)
        app.run(config('test_host', config('host')), config('test_port', config('port')), debug=True)
