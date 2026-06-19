"""Continuously ingest fake data into a local database.

Run from the repo root (after running examples/bootstrap_db.py):

    python examples/insert_loop.py

Sends one influx-style data point per second. Press Ctrl+C to stop.

The script writes directly to the local database (no HTTP server
needed) via ong_tsdb.database.OngTSDB.write_tick_numpy.
"""

import os
import time

import numpy as np

from ong_tsdb import BASE_DIR
from ong_tsdb.database import OngTSDB


def main():
    target = os.path.join(BASE_DIR, "test_bootstrap")
    if not os.path.isfile(os.path.join(target, "CONFIG.JSON")):
        print(f"No database found at {target}.")
        print("Run examples/bootstrap_db.py first.")
        return

    print(f"Ingesting into {target}.")
    print("Press Ctrl+C to stop.")
    print()

    db = OngTSDB(path=target)
    with open(db.FU.path_config()) as f:
        admin = f.readline().strip()

    # The bootstrap script created database 'demo' with sensor 'm1'
    # and metrics ['active', 'reactive']. We mirror that here.
    DB = "demo"
    SENSOR = "m1"

    try:
        i = 0
        while True:
            i += 1
            # Fake data: active oscillates, reactive increases
            active = 100 + (i % 50)
            reactive = 200 + i
            np_values = np.array([[float(active), float(reactive)]], dtype=np.float32)
            np_timestamps = np.array([time.time() + i * 0.001], dtype=np.float64)
            db.write_tick_numpy(admin, DB, SENSOR, np_values, np_timestamps)
            print(f"[{i}] active={active} reactive={reactive}")
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
