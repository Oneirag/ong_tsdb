"""Bootstrap a fresh ong_tsdb database for local experimentation.

Run from the repo root:

    python examples/bootstrap_db.py

What it does:
    1. Creates BASE_DIR/test_bootstrap/ and writes an auto-generated
       admin key to <BASE_DIR>/test_bootstrap/CONFIG.JSON. The key is
       printed once to stdout -- copy it, then read the file or
       CONFIG.JSON to recover it later.
    2. Creates a database called 'demo' and a sensor 'm1' with
       1-second frequency and two metrics.
    3. Lists the resulting structure.

This is the smallest end-to-end example of using the ong_tsdb
package from Python (without going through the HTTP server).
"""

import os
import time

from ong_tsdb import BASE_DIR, config
from ong_tsdb.database import OngTSDB


def main():
    target = os.path.join(BASE_DIR, "test_bootstrap")
    print(f"Bootstrapping database at {target}")
    print(f"Make sure {target} does NOT exist beforehand, or it will be reused.")
    print()

    db = OngTSDB(path=target)

    # Read the admin key from the freshly-written CONFIG.JSON
    with open(db.FU.path_config()) as f:
        admin = f.readline().strip()
    print(f"Admin key (save it, it is NOT logged anywhere): {admin}")
    print()

    print("Creating database 'demo' ...")
    db.create_db(admin, "demo")
    print("Creating sensor 'm1' (1s freq, metrics: ['active', 'reactive']) ...")
    db.create_sensor(
        admin,  # admin_key
        "demo",  # db
        "m1",  # sensor
        "1s",  # period
        admin,  # write_key (reuse admin for the demo)
        admin,  # read_key
        ["active", "reactive"],  # metrics
    )
    print()
    print("Done. Structure:")
    for db_name in db.FU.getdbs():
        for sensor in db.FU.getsensors(db_name):
            print(f"  {db_name}/{sensor}")
    print()
    print("Next: run examples/insert_loop.py to ingest data, then")
    print("      examples/verify_chunks.py to inspect the chunks.")


if __name__ == "__main__":
    main()
