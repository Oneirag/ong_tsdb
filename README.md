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
This package uses ong_utils for configuration, that will be search in 
`~/.config/ongpi/ong_tsdb.yml`
The minimal configuration would be:
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
Run server.py 

`python3 -m ong_tsdb.server`

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


### Ingesting data
Create a client for writing and send data, as a str (for a single measurement), a list of
str or a list of tuples. 
When ingesting a lot of data at once, using list of tuples is obviously faster
```python
import pandas as pd
from ong_tsdb import config
from ong_tsdb.client import OngTsdbClient

# With admin key would also work. Otherwise, errors for lack of permissions would be risen
write_client = OngTsdbClient(url=config('url'), token=config('write_token'))

ts = pd.Timestamp.now().value
sequence_str = f"my_db,sensor=my_sensor measurement1=123,measurement2=34.4 {ts}"
sequence_list = [("my_db", "my_sensor", ["measurement1", "measurement2"], [123, 34.4], ts)]
write_client.write(sequence_str)
write_client.write(sequence_list)       # Same but with a list
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
