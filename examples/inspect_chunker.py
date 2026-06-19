"""Benchmark the ong_tsdb influx line parser.

Run from the repo root:

    python examples/inspect_chunker.py

The influx line format is:

    <db>,<ignored_key>=<sensor> <metric1>=<v1>,<metric2>=<v2>,... <timestamp_ns>

The line is parsed by ong_tsdb.server_utils.split_influx into a
5-tuple: (db, sensor, metric_names, values, ts). This script
parses 65536 synthetic lines and reports the elapsed time.
"""

from timeit import timeit

from ong_tsdb.server_utils import InfluxParseException, split_influx


def main():
    line = (
        "database,key_to_ignore=sensor "
        "metric_name=1.1,metric_name2=2.2,metric_name3=3.3,"
        "metric_name4=4.4,metric_name5=5.5 "
        "1637262558914122000"
    )
    number = 2**16

    # Warm-up
    try:
        split_influx(line)
    except InfluxParseException as e:
        print(f"Parser error during warm-up: {e}")
        return

    elapsed = timeit(
        "split_influx(line)",
        globals={"line": line, "split_influx": split_influx},
        number=number,
    )
    print(
        f"split_influx: {elapsed:.4f} s for {number} iterations "
        f"({elapsed / number * 1e6:.2f} us/call)"
    )

    # Example output
    print()
    print("Example parse:")
    db, sensor, metrics, values, ts = split_influx(line)
    print(f"  db     = {db}")
    print(f"  sensor = {sensor}")
    print(f"  metrics= {metrics}")
    print(f"  values = {values}")
    print(f"  ts     = {ts}")


if __name__ == "__main__":
    main()
