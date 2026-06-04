import time


class InfluxParseException(Exception):
    pass


def split_influx(line: str) -> tuple:
    """Splits an influx line into a tuple consisting of:
    db (str), sensor (str), metrics (list of str), values (list of float) and ts (float in nanos)
    Raises InfluxParseException if line could not be parsed
    """
    try:
        first_split = line.split(" ")
        if len(first_split) < 3:
            first_split.append(time.time_ns())  # If ts is missing append it
        db_sensor, metrics, ts = first_split
        db, sensor_key = db_sensor.split(",")[:2]
        sensor = sensor_key.split(",")[0].split("=")[1]
        metrics = [metric.split("=") for metric in metrics.split(",")]
        data_metrics = [m for m, v in metrics]
        data_values = [float(v) for m, v in metrics]
        return db, sensor, data_metrics, data_values, float(ts)
    except Exception as e:
        raise InfluxParseException(f"Error parsing line {line}: {e}")
