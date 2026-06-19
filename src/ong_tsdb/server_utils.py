import re
import time
from timeit import timeit

# re_influx accepts just one sensor and all metrics are extracted to be parsed with re_metrics
re_influx = re.compile(r"(?P<db>\w+)(,?\w+=(?P<sensor>\w+))? (?P<metrics>\S+) (?P<ts>\d+)")
re_metrics = re.compile(",?([^=]+)=([^,]+)")


class InfluxParseException(Exception):
    pass


def regex_split(line):
    influx_dict = re_influx.match(line).groupdict()
    # print(pd.Timestamp.utcfromtimestamp(int(influx_dict['ts']) / 1e9))
    metrics_tick = {k: float(v) for (k, v) in re_metrics.findall(influx_dict['metrics'])}
    return influx_dict, metrics_tick


def split_influx(line: str) -> tuple:
    """Splits an influx line into a tuple consisting of:
        db (str), sensor (str), metrics (list of str), values (list of float) and ts (float in nanos)
        Raises InfluxParseException if line could not be parsed
    """
    try:
        first_split = line.split(" ")
        if len(first_split) < 3:
            first_split.append(time.time_ns())      # If ts is missing append it
        db_sensor, metrics, ts = first_split
        db, sensor_key = db_sensor.split(",")[:2]
        sensor = sensor_key.split(",")[0].split("=")[1]
        metrics = [metric.split("=") for metric in metrics.split(",")]
        data_metrics = [m for m, v in metrics]
        data_values = [float(v) for m, v in metrics]

        # metrics = {k: v for k, v in metrics}
        return db, sensor, data_metrics, data_values, float(ts)
    except Exception as e:
        raise InfluxParseException(f"Error parsing line {line}: {e}")


if __name__ == '__main__':
    line = 'database,key_to_ignore=sensor,key_to_ignore=other_sensor metric_name=1.1,metric_name2=2.2,metric_name3=3.3 1355'
    line = 'database,key_to_ignore=sensor metric_name=1.1,metric_name2=2.2,metric_name3=3.3,metric_name3=3.3,metric_name3=3.3,metric_name3=3.3 1355'
    line = 'database,key_to_ignore=sensor metric_name=1.1 1355'
    # line = 'database,key_to_ignore=sensor metric_name=1.1,metric_name2=2.2,metric_name3=3.3 1355'
    number = 2**16

    for f in split_influx, regex_split:
        print(f.__name__)
        print(f(line))
    print(split_influx(line))
    print(timeit("split_influx(line)", globals=globals(), number=number))
    print(timeit("regex_split(line)", globals=globals(), number=number))
