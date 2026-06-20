# Ong_tsdb
A file-based time series database for fixed interval data, meant for managing data received in regular 
intervals (such as meter data, stock data, power market prices...).

It is aimed at running in cheap hardware (e.g. raspberry pi, low-end mini pc...) with very little CPU usage and
memory footprint (unless you are not receiving lots of data), and as it is file-based it is easy to back up just by 
copying directories.

However, due to its simplicity it has a lot of disadvantages:
* is rather **unsecure**
* **not thread-safe**
* **non-distributed**
* it might **not be able to manage a great load** of ingested data at once
* it is **not probably the fastest** DB in the world
* it writes every tick to the HD, so if using a SD or SDD it can reduce its expected live 

Therefore, it is **only recommended for personal use in DIY projects**.

Data is stored in files in a directory, where each file is a numpy array of fixed number of rows, 
each row corresponding to a specific time interval and has a column per metric/measurement.

Each file is called chunk. In order to save disk space, chunks use small dtype (np.float32) 
and old files are gzipped. For each chunk, the number of measurements/metrics is the extension of the file

## How data is stored
Data is organized in binary files in directories, that are created in two levels of subdirectories of
`BASE_DIR`.
+ The first level is the database name.
+ The second level are the sensors. One database can have many sensors. Each sensor has its own configuration (frequency, write and read tokes)
+ Inside a sensor directory, chunk files are located. Chunk files contain data for measurements associated with a specific sensor

Some examples of use cases:
+ db=meter, sensor=mirubee, metrics=['active', 'reactive'] for storing real time data from a mirubee device in a meter
+ db=NYSE, sensor=APPLE, metrics=['Open', 'High', 'Low', 'Close'] for storing apple stock data
+ db=stocks, sensor=NYSE, metrics=['APL.O', 'APL.H', 'APL.L', 'APL.C'] is another way of storing the above-mentioned information
+ db=stocks, sensor=NYSE, metrics=[['APL', 'Open'], ['APL','High'], ['APL', 'Low'], ['APL', 'Close']] uses multiindex 
to manage the stock data. In such case the df returned will have two levels in columns, but using pandas functions it will
make easier to manipulate in case of several stocks read from NYSE

### Chunk file structure
A Chunk is a fixed-row numpy matrix of `dtype=np.float32` (to save disk storage). Currently,
each chunk has 2^14 rows. As the number of rows and the frequency of the sensor if fixed, each
chunk represent a fixed amount of time and each row represent a specific time. E.g. if frequency is 3s,
each chunk represent 3 * 2^14 seconds in total, and each row will represent the data 3 seconds after
the data of the preceding row.

Chunk names have the form: `{start_timestamp}.{number_of_columns}[.gz]`:
+ Start_timestamp are calculated provided that for timestamp=0 a start_timestamp=0, so for any given timestamp, its start_timestamp
is `int(timestamp / chunk_duration) * chunk_duration`
+ number_of_columns represent the number of measurements/metrics that this chunk stores
+ if chunk name ends by `.gz` that means that it is compressed.
The matrix inside the chunk is a numpy array with number_of_columns+2 columns (two extra columns: index and checksum). 
The columns inside the chuk file are:
+ Column 1: represent the row number + 1, or 0 if there is no data in that row.
+ Columns 2:-1: here the metrics/measurements are stored, in the same order as the metrics/measurements where defined.
+ Column -1: is a checksum, result of adding row[1:-1], useful to check data integrity

## How to use it
### Create configuration files
This package uses ong_utils for configuration, that by default will be searched in 
`~/.config/ongpi/ong_tsdb.yml`.
In case you need the file to be placed in other directory (e.g. if using google colab and want the file to persist), 
create an environment variable called `ONG_CONFIG_PATH` that points to the directory where `ong_tsdb.yml` will be 
located. For example, to use a config file named `/content/gdrive/MyDrive/.config/ongpi/ong_tsdb.yml` in google colab use:
```shell
%env ONG_CONFIG_PATH=/content/gdrive/MyDrive/.config/ongpi
```


The minimal configuration of the `ong_tsdb.yml` file would be:
```yaml
ong_tsdb:
    BASE_DIR: ~/Documents/ong_tsdb   # Location to store files
    port: 5000                        # Port for flask server only
    host: 127.0.0.1                   # host for flask server only
    url: http://localhost:5000        # url for client. Include port if needed 
    admin_token: whatever_you_want_here 
    read_token: whatever_you_want_here
    write_token: whatever_you_want_here
    uncompressed_chunks: 10         # number of files to remain uncompressed. Use -1 to disable compression
```


### Create server and run it
Run server.py with python

```shell
python3 -m ong_tsdb.server`
```
or execute a script (if path is properly configured)
```shell
ong_tsdb_server
```
Both are equivalent

### Create server and run it (in google colab)
This section explains how to create a persistent server in google colab that persists data in google drive.

First, mount google drive, install library and set environment variable to point to google drive.
````python
# Mount google drive
from google.colab import drive
drive.mount('/content/gdrive')

# install ong_tsdb
!pip install git+https://github.com/Oneirag/ong_tsdb.git
# set environ variable to make config persistent
%env ONG_CONFIG_PATH=/content/gdrive/MyDrive/.config/ongpi
````
If it is the first execution, edit the config file `/content/gdrive/MyDrive/.config/ongpi/ong_tsdb.yaml` 
with this recommended configuration:
```yaml
log: 
ong_tsdb: 
  host: localhost
  port: 5000
  BASE_DIR: /content/gdrive/MyDrive/ong_tsdb
  admin_token: whatever_you_want_here 
  read_token: whatever_you_want_here
  write_token: whatever_you_want_here
  uncompressed_chunks: 10
```

Then, to start server in background, in a single cell run:
```python
%%python3 --bg --out output

from ong_tsdb.server import main
main()
```

### Import client, create db and sensors
```python
from ong_tsdb import config
from ong_tsdb.client import OngTsdbClient

admin_client = OngTsdbClient(url=config('url'), token=config('admin_token'))
admin_client.create_db("name_of_database")
# Create a sensor that stores data every second
admin_client.create_sensor("name_of_database", "name_of_sensor", "1s", list(),
                           read_key=config("read_token"), write_key=config("write_token"))

# Create a sensor that stores data every two minutes
admin_client.create_sensor("name_of_database", "name_of_sensor_two_mins", "2m", list(),
                           read_key=config("read_token"), write_key=config("write_token"))

# Create a sensor that stores data every week
admin_client.create_sensor("name_of_database", "name_of_sensor_wk", "7d", list(),
                           read_key=config("read_token"), write_key=config("write_token"))

# Metrics can be a list. In such cases dataframes are multiindex dataframes
admin_client.create_sensor("name_of_database", "name_of_sensor_wk", "7d", [["A", "B", "C"]],
                           read_key=config("read_token"), write_key=config("write_token"))

```
### Remote authentication
If client is in a remote server that needs its own authentication previous to the ongtsdb_authentication, this can be 
done by supplying the extra authentication parameters for a post request (as a dict) with the `proxy_auth_body` 
parameter

```python
from ong_tsdb import config
from ong_tsdb.client import OngTsdbClient

admin_client = OngTsdbClient(url=config('url'), token=config('admin_token'),
                             proxy_auth_body=dict(username=config("username"),
                                                  password=config("password")))

```
### Reusing client connection with different token
If a client connection wants to be reused with a different token, e.g. for being behind a proxy
with a TOTP MFA authentication and do not want to wait for the new token generation,
the `update_token` method can be used:

```python
from ong_tsdb import config
from ong_tsdb.client import OngTsdbClient

client = OngTsdbClient(url=config('url'), token=config('admin_token'),
                             proxy_auth_body=dict(username=config("username"),
                                                  password=config("password"),
                                                  mfa=input("MFA code")))
# Do whatever with the admin client
# ...

client.update_token(config("read_token"))   # Now client is a read client, reusing connection
# Do whatever with the read client
# ...
```



### Ingesting data
Create a client for writing and send data, as a str (for a single measurement), a list of
str or a list of tuples. 
When ingesting a lot of data at once, using list of tuples is obviously faster

If you append data that it is new, a new measurement is created. Not filled values are filled
by default with 0, but it can be filled with any other value with the `fill_value` parameter
of `write` or `write_df` methods.
```python
import pandas as pd
import numpy as np
from ong_tsdb import config
from ong_tsdb.client import OngTsdbClient

# With admin key would also work. Otherwise, errors for lack of permissions would be risen
write_client = OngTsdbClient(url=config('url'), token=config('write_token'))

ts = pd.Timestamp.now().value
sequence_str = f"my_db,sensor=my_sensor measurement1=123,measurement2=34.4 {ts}"
sequence_list = [("my_db", "my_sensor", ["measurement1", "measurement2"], [123, 34.4], ts)]
write_client.write(sequence_str)
write_client.write(sequence_list)       # Same but with a list
# Write new data
new_sequence_list = [("my_db", "my_sensor", ["new_measurement"], [123], ts + 1)]
# In this case, past values will be filled with nan instead of 0s
write_client.write(new_sequence_list, fill_value=np.nan)

import pandas as pd
df = pd.DataFrame([[123, 34.4]], index=[pd.Timestamp.utcfromtimestamp(ts/1e9)], 
                  columns=["measurement1", "measurement2"])
write_client.write_df("my_db", "my_sensor", df)       # Same with pandas dataframe


```

### Reading data
Create a client with read credentials and call `read` method
```python
import pandas as pd
from ong_tsdb import config
from ong_tsdb.client import OngTsdbClient

# With admin key would also work. Otherwise, errors for lack of permissions would be risen
read_client = OngTsdbClient(url=config('url'), token=config('read_token'))

# Read data into pandas dataframe, columns are metric names and indexed by data in LOCAL_TZ
df = read_client.read("my_db", "my_sensor", pd.Timestamp(2021,10,10))   # All measurements/metrics
df =read_client.read("my_db", "my_sensor", pd.Timestamp(2021,10,10), metrics=['my_metric'])     # Just "my_metric"
df =read_client.read("my_db", "my_sensor", pd.Timestamp(2020,10,10), pd.Timestamp(2021, 10, 10))     # 1 year data

# In case a large extraction from local db is foreseen, a faster alternative would be:
# Works only if database is located in the same machine as client, and user has access to files
df =read_client.local_read("my_db", "my_sensor", pd.Timestamp(2020,10,10), pd.Timestamp(2021, 10, 10))     # 1 year data

```

### Multiindex
Ong_tsdb can manage multiindex dataframes by treating measurements as a list of strings.
If multiindex has level names and Ong_tsdb must be aware of them, either:
* Call create_sensor using level_names parameters
* Call create_sensor using metadata parameter with a dictionary having a key "level_names" with the level names (same as above)
* Call client.set_level_names for an existing sensor with the new level_names

See the following example for creating a multiindex sensor with either level_names or metadata:
```python
import pandas as pd
from ong_tsdb import config
from ong_tsdb.client import OngTsdbClient

admin_client = OngTsdbClient(url=config('url'), token=config('admin_token'))

admin_client.create_db("name_of_database")
# Create a sensor that stores data every second. 
# As measurements is a list of list, it will create just be one measurement, indexed by ("value1", "value2")
admin_client.create_sensor("name_of_database", "name_of_sensor", "1s", [['value1', 'value2']],
                           read_key=config("read_token"), write_key=config("write_token"))

# Same as above, but now level_names for the multiindex are informed. That will make multiindex easier to 
# query as a dataframe
admin_client.create_sensor("name_of_database", "name_of_sensor", "1s", [['value1', 'value2']],
                           read_key=config("read_token"), write_key=config("write_token"), 
                           level_names=["one", "another"])
# Exactly the same as above, but a little more verbose
admin_client.create_sensor("name_of_database", "name_of_sensor", "1s", [['value1', 'value2']],
                           read_key=config("read_token"), write_key=config("write_token"), 
                           metadata=dict(level_names=["one", "another"]))
```

## Relation to influxdb
Ong_tsdb resembles some concepts of [influxdb](https://www.influxdata.com/):
+ Data is stored in databases (buckets in influx)
+ For each database, sensors can be defined. A sensor is defined with a name, a token to read, a token to write and its fixed frequency
+ Dates for data ingestion are given as nanosecond timestamps
+ For each sensor many data points can be defined, all sharing the same chunks
+ Data can be ingested using influx format. For example `bucket,sensor=sensor_name measurement1=12,measurement2=333.3 1637262558914122000` will
store in database bucket, sensor sensor_name values 12 and 333.3 for `measurement1` and `measurement2` in timestamp `1637262558914122000`
+ There is a client that has a write method accepting strings in influx format  (see above) or the same information in binary format
  (as lists of tuples formed by db_name, sensor_name, list_of_measurement_names, list_of_measurement_values, timestamp_nanos)
However, ong_tsdb does not include query language. Information can be retrieved in just numpy or pandas arrays.

## Integration with grafana
Ong_tsdb can be read from a [grafana](https://grafana.com/) dashboard. 

To do so, create one data source for each sensor you want to read using a [simple-json-datasource](https://github.com/grafana/simple-json-datasource?utm_source=grafana_add_ds) 

To configure the datasource:
+ URL: `{ong_tsdb_server_url}:{ong_tsdb_server_port}/{db_name}/{sensor_name}`, eg http://localhost:5000/example_db/example_sensor
+ Auth: check (activate) "Basic auth"
+ In the basic auth details
  + User: whatever you want, is not used
  + Password: the token you want to use. It can be the read_token, write_token or admin_token. No other will work

These data sources can be used in any grafana chart

## Database maintenance

The package ships with two CLI subcommands for inspecting and repairing
chunk files without needing to write Python:

```shell
python -m ong_tsdb <subcommand> [options]
```

### Why chunks may become corrupt

Chunk files are written with a single `f.write(value_write.tobytes())`
call. If the process is killed (OOM, power loss, manual `kill`, SD-card
glitch) between the moment the file is opened and the moment the
buffer is flushed, the file is left truncated. Reads on a truncated
file used to crash with a bare `cannot reshape array of size X into
shape (Y,Z)` message; since v0.8.0 the read path catches that and
skips the bad chunk, and a CLI tool is provided to actually fix the
file.

### `verify` — scan for corrupt chunks

```shell
# Verbose scan (every chunk + per-sensor summary)
python -m ong_tsdb verify

# Just the corrupt list, with full context (path, expected/actual
# bytes, missing delta, previous chunk timestamp for reference)
python -m ong_tsdb verify --corrupt-only

# Add a progress bar (tqdm if installed, else a stdlib fallback)
python -m ong_tsdb verify --corrupt-only --progress

# Restrict the scan to a single database
python -m ong_tsdb verify --db mydb

# Override BASE_DIR
python -m ong_tsdb verify --base-dir /path/to/ong_tsdb
```

Exit code is 0 when no chunk is corrupt, 1 otherwise (suitable for
shell scripts and CI smoke tests).

### `repair` — auto-fix truncated chunks

For each corrupt chunk found by `verify`, the repair tool:

1. Reads the complete rows from the truncated file (the trailing
   partial row, if any, is discarded).
2. Validates the checksums of the data rows (the same `nansum`
   check used by the read path). If a recovered row's checksum
   does not match, the file is left untouched.
3. Rebuilds the file to the correct `CHUNK_ROWS × (n_metrics + 2)`
   size by padding the missing rows with `NaN` (the same convention
   used for unwritten rows in a fresh chunk).
4. Renames the original to `<file>.corrupt.bak` so it can be
   restored if anything looks wrong after the fix.
5. Writes the repaired file atomically (`safe_createfile`,
   i.e. `<file>.tmp.<pid>` + `os.replace`).

```shell
# Preview what would be repaired (no writes)
python -m ong_tsdb repair --db mydb --dry-run

# Repair everything that can be safely repaired
python -m ong_tsdb repair --db mydb

# Repair without keeping the original at <file>.corrupt.bak
python -m ong_tsdb repair --db mydb --no-backup
```

Example output:

```
Found 2 corrupt chunk(s).
  /home/.../meter/mirubee/1715044352.10
    Corrupt chunk ...: expected 163840 elements (16384 rows x 10 cols, ...,
    655360 bytes), got 160768 elements (643072 bytes). Missing 3072
    elements (12288 bytes). Likely a truncated write.
  /home/.../meter/seeedstudio/1769832448.6
    ...

  REPAIR  /home/.../meter/mirubee/1715044352.10
          kept 16076 row(s), padded 308 NaN row(s)
  REPAIR  /home/.../meter/seeedstudio/1769832448.6
          kept 16213 row(s), padded 171 NaN row(s)

Repaired 2 chunk(s), skipped 0.
Originals preserved at <file>.corrupt.bak
=== All 2 repaired file(s) now pass fast_read_np ===
```

Only the files that were touched are re-verified after the repair, so
the command stays fast even on large databases (a full-database
re-scan could take minutes if you have many sensors or years of data).

### Recommended workflow

```shell
# 1. Detect
python -m ong_tsdb verify --corrupt-only

# 2. Preview
python -m ong_tsdb repair --db mydb --dry-run

# 3. Repair
python -m ong_tsdb repair --db mydb

# 4. Confirm
python -m ong_tsdb verify --corrupt-only
# -> exit 0, "All chunks OK"
```

Once you have confirmed the data looks right, the `.corrupt.bak` files
can be removed with a simple
`find BASE_DIR -name "*.corrupt.bak" -delete`.

## Changelog

### 0.9.2
This is a correctness release that closes two `NameError` holes that
slipped through the 0.9.0 → 0.9.1 cycle because of a partial revert
(commit `9998159`, "Revert Release/0.9.0") followed by an
incomplete re-merge (PR #29). After `pip install ong_tsdb==0.9.1` the
installed `src/ong_tsdb/server.py` ended up calling `_get_db()` 29
times without the helper ever being defined; **every** request to
`/influx`, `/influx_binary`, `/read_df`, `/metrics`, `/metadata`, etc.
raised `NameError: name '_get_db' is not defined` and returned HTTP
500. Reinstalling from `master` did not recover a working state
because the bug is on `master` itself.

The fix is two small surgical changes; no behaviour change relative
to the post-0.9.0 design intent.

- **fix**: re-introduce the `def _get_db():` lazy-init helper in
  `src/ong_tsdb/server.py` and switch the module-level `_db` from
  eager (`_db = OngTSDB()`) to lazy (`_db = None`). The helper is the
  single point that constructs the `OngTSDB` instance on first use.
  This is the exact block that the partial revert of `1eef9a7`
  removed; re-adding it makes the 29 call sites resolvable again.
  The 29 call sites themselves (which the partial revert left in
  place) are correct and untouched.
- **fix**: `OngTSDB.__init__` now bootstraps a fresh config file at
  the requested `path` instead of the global `BASE_DIR`. The previous
  implementation did `os.makedirs(BASE_DIR)` and `FileUtils()` with
  defaults, which silently overwrote the user's actual database root
  whenever a non-default `path` was passed (e.g. from a test fixture
  or a script pointing at a per-project data dir). Replaced with
  `os.makedirs(path, exist_ok=True)` and a `self.FU`-rooted config
  write. The error was masked by the `_get_db` failure (which fired
  first) but became visible the moment `_get_db` was restored.
  (`src/ong_tsdb/database.py`)
- **fix**: every remaining `_db.xxx(...)` call site in
  `src/ong_tsdb/server.py` is now `_get_db().xxx(...)`. The partial
  revert of the lazy-init refactor left 25 call sites on the bare
  `_db` global while only ~4 had been renamed to `_get_db()`. After
  the helper was re-added, those bare references still resolved
  (the name exists) but pointed at `None` (the lazy initial state),
  so the first request to any of them raised
  `AttributeError: 'NoneType' object has no attribute 'X'` and
  returned HTTP 500 — including, importantly, the grafana `/query`
  endpoint and several other read paths that the previous fix
  did not exercise. The 25 sites have been mechanically rewritten
  in place; the only remaining mentions of `_db` in the file are
  inside the lazy-init helper itself.
- **test**: `tests/test_server_module_layout.py` now contains five
  smoke tests that pin the lazy-init contract end to end:
    1. `ong_tsdb.server._get_db` is defined and callable.
    2. The module-level `_db` is `None` at import time (lazy, not
       eager).
    3. `_get_db()` constructs `OngTSDB` exactly once and caches the
       instance in the module-level `_db`.
    4. **Static check**: no source line outside the lazy-init helper
       references the bare `_db` global. The check locates the
       helper block by its `_db = None` ... `return _db` markers and
       rejects any other mention. This would have caught the bare
       `_db.xxx()` regression at CI time.
    5. **HTTP regression**: a real `POST` to the grafana `/query`
       endpoint (the path that produced
       `AttributeError: 'NoneType' object has no attribute
       'get_metrics'` in production) must not return 500 and must
       not mention `NoneType` in the body.

**No public API change.** Wire format, on-disk format, route paths,
and HTTP behaviour are unchanged relative to 0.9.1.

**What this release does NOT do.** The 0.9.0 release also added a
large corruption-resilience feature set (`verify`, `repair`, chunk
checksums, gevent-aware HTTP, dedicated unit tests, and the
`COMPRESSION_ZSTD` default for new chunks). Most of that work was
removed from `master` by the partial revert and is **not** restored
by 0.9.2. This release only recovers the runtime behaviour that makes
`/influx` ingest data again. If you depend on the verify/repair CLI
or on zstd compression for new chunks, that work is tracked
separately and has not been re-merged at the time of 0.9.2.

### 0.9.1
- **fix**: `POST /influx` and `POST /influx_binary` raised `NameError:
  name 'metrics_get_db' is not defined` on every row. The bug was
  introduced by the lazy `OngTSDB` initialization refactor in 0.9.0,
  where a mass rename of the private `_db` symbol to `_get_db()`
  accidentally corrupted the local `metrics_db` variable inside
  `write_point_list`. Restored to the pre-refactor behaviour; no
  public API change. (`src/ong_tsdb/server.py:289`)
- **test**: added `tests/test_write_point_list.py` with regression
  tests for `write_point_list` and the `/influx` endpoint, so this
  regression cannot reappear silently.

### 0.9.0
- Default chunk compression switched from gzip to zstd (~25% better
  ratio, 4-12x faster compress/decompress on representative data).
  Old gzip chunks continue to read transparently and migrate as they
  are rewritten.

