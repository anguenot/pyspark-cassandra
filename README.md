PySpark Cassandra
=================

[![APACHE2
License](https://img.shields.io/badge/license-Apache2.0-blue.svg?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

`pyspark-cassandra` is a Python port of the [DataStax Cassandra
Connector](https://github.com/datastax/spark-cassandra-connector).

This module provides Python support for Apache Spark's Resilient Distributed
Datasets from Apache Cassandra CQL rows using [Cassandra Spark
Connector](https://github.com/datastax/spark-cassandra-connector) within
PySpark, both in the interactive shell and in Python programs.

This project was initially forked from
[@TargetHolding](https://github.com/TargetHolding/pyspark-cassandra) since they
no longer maintain it.

**Contents:**
* [Compatibility](#compatibility)
* [Installation](#installation)
* [Usage](#usage)
* [Building](#building)
* [API](#api)
* [Examples](#examples)
* [Problems / ideas?](#problems--ideas)
* [Contributing](#contributing)


Compatibility
-------------

| Component | Version |
|-----------|---------|
| Spark | 3.5.x |
| Scala | 2.12 |
| Java | 17 |
| spark-cassandra-connector | 3.5.1 |
| Cassandra | 3.11.x, 4.0.x, 4.1.x |
| Python | 3.9+ |


Installation
------------

```bash
pip install pyspark-cassandra
```

The package bundles the required JVM assembly JAR. No separate JAR download or
`--packages` flag is needed.


Usage
-----

### Automatic JAR configuration

If a `SparkContext` is already active when you import `pyspark_cassandra`, the
bundled JAR is automatically added via `sc.addJar()`:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.cassandra.connection.host", "localhost") \
    .getOrCreate()

import pyspark_cassandra  # JAR is auto-added to the active SparkContext
```

### Configuring before SparkContext creation

If you need to configure the JAR before creating the SparkContext (e.g. for
cluster mode), use `pyspark_cassandra.jar_path()`:

```python
import pyspark_cassandra
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.jars", pyspark_cassandra.jar_path()) \
    .config("spark.cassandra.connection.host", "localhost") \
    .getOrCreate()
```

### With spark-submit

```bash
spark-submit \
    --jars $(python -c "import pyspark_cassandra; print(pyspark_cassandra.jar_path())") \
    --conf spark.cassandra.connection.host=your,cassandra,node,names \
    yourscript.py
```

### PySpark shell

```bash
pyspark \
    --jars $(python -c "import pyspark_cassandra; print(pyspark_cassandra.jar_path())")
```

Then in the shell:

```python
import pyspark_cassandra
```

Note that when performing this import the `sc` variable in pyspark is augmented
with the `cassandraTable(...)` method.


Building
--------

### Prerequisites

* Java 17
* SBT 1.10+
* Python 3.11

### Build the assembly JAR and package

```bash
make dist
```

This compiles the Scala bridge, creates the fat JAR (excluding Spark/Scala which
are provided at runtime), and copies it into
`python/pyspark_cassandra/jars/`.

### Build a distributable wheel

```bash
make dist
cd python && python -m build
```

The wheel at `python/dist/pyspark_cassandra-*.whl` includes the JAR and can be
installed with `pip install`.


API
---

The PySpark Cassandra API aims to stay close to the Cassandra Spark Connector
API. Reading its
[documentation](https://github.com/datastax/spark-cassandra-connector/#documentation)
is a good place to start.


### pyspark_cassandra.RowFormat

The primary representation of CQL rows in PySpark Cassandra is the ROW format.
However `sc.cassandraTable(...)` supports the `row_format` argument which can be
 any of the constants from `RowFormat`:
* `DICT`: A CQL row is represented as a python dict with the CQL row columns as
keys.
* `TUPLE`: A CQL row is represented as a python tuple with the values in CQL
table column order / the order of the selected columns.
* `ROW`: A pyspark_cassandra.Row object representing a CQL row (default).

Column values are related between CQL and python as follows:

|  **CQL**  |       **python**      |
|:---------:|:---------------------:|
|   ascii   |          str          |
|   bigint  |          int          |
|    blob   |       bytearray       |
|  boolean  |         bool          |
|  counter  |          int          |
|  decimal  |        decimal        |
|   double  |         float         |
|   float   |         float         |
|    inet   |          str          |
|    int    |          int          |
|    map    |         dict          |
|    set    |          set          |
|    list   |         list          |
|    text   |          str          |
|    date   |   datetime.date       |
| timestamp |   datetime.datetime   |
|  timeuuid |       uuid.UUID       |
|  varchar  |          str          |
|   varint  |          int          |
|    uuid   |       uuid.UUID       |
|   _UDT_   | pyspark_cassandra.UDT |


### pyspark_cassandra.Row

This is the default type to which CQL rows are mapped. It is directly compatible
 with `pyspark.sql.Row` but is (correctly) mutable and provides some other
 improvements.


### pyspark_cassandra.UDT

This type is structurally identical to pyspark_cassandra.Row but serves user
defined types. Mapping to custom python types (e.g. via CQLEngine) is not yet
supported.


### pyspark_cassandra.CassandraSparkContext

A `CassandraSparkContext` is very similar to a regular `SparkContext`. It is
created in the same way, can be used to read files, parallelize local data,
broadcast a variable, etc. See the [Spark Programming Guide](https://spark.apache.org/docs/latest/programming-guide.html)
for more details. *But* it exposes one additional method:

* ``cassandraTable(keyspace, table, ...)``:	Returns a CassandraRDD for the given
 keyspace and table. Additional arguments which can be provided:

  * `row_format` can be set to any of the `pyspark_cassandra.RowFormat` values (defaults to `ROW`)
  * `split_size` sets the size in the number of CQL rows in each partition (defaults to `100000`)
  * `fetch_size` sets the number of rows to fetch per request from Cassandra (defaults to `1000`)
  * `consistency_level` sets with which consistency level to read the data (defaults to `LOCAL_ONE`)
  * `connection_config` map with connection information that can be used to connect non-default cluster (defaults to None)


### pyspark.RDD

PySpark Cassandra supports saving arbitrary RDD's to Cassandra using:

* ``rdd.saveToCassandra(keyspace, table, ...)``: Saves an RDD to Cassandra. The
RDD is expected to contain dicts with keys mapping to CQL columns. Additional
arguments which can be supplied are:

  * ``columns(iterable)``: The columns to save, i.e. which keys to take from the dicts in the RDD.
    * ``array``: list of columns to be saved with default behaviour (overwrite)
    * ``dictionary``: list of columns (keys) and cassandra collections modify operation to perform (values)
    -``""`` : emulate 'array' default behaviour. to be used for non-collection fields
    -``"append" | "add"`` : append to a collection (lists, sets, maps)
    -``"prepend"`` : prepend to a collection (lists)
    -``"remove"`` : remove from a collection (lists, sets)
    -``"overwrite"`` : overwrite a collection (lists, sets, maps)
  * ``batch_size(int)``: The size in bytes to batch up in an unlogged batch of
  CQL inserts.
  * ``batch_buffer_size(int)``: The maximum number of batches which are
  'pending'.
  * ``batch_grouping_key(string)``: The way batches are formed (defaults
  to "partition"):
     * ``all``: any row can be added to any batch
     * ``replicaset``: rows are batched for replica sets
     * ``partition``: rows are batched by their partition key
  * ``consistency_level(cassandra.ConsistencyLevel)``: The consistency level
  used in writing to Cassandra.
  * ``parallelism_level(int)``: The maximum number of batches written in
  parallel.
  * ``throughput_mibps``: Maximum write throughput allowed per single core in
  MB/s.
  * ``ttl(int or timedelta)``: The time to live as milliseconds or timedelta to
  use for the values.
  * ``timestamp(int, date or datetime)``: The timestamp in milliseconds, date
  or datetime to use for the values.
  * ``metrics_enabled(bool)``: Whether to enable task metrics updates.


### pyspark_cassandra.CassandraRDD

A `CassandraRDD` is very similar to a regular `RDD` in pyspark. It is extended
with the following methods:

* ``select(*columns)``: Creates a CassandraRDD with the select clause applied.
* ``where(clause, *args)``: Creates a CassandraRDD with a CQL where clause
applied. The clause can contain ? markers with the arguments supplied as *args.
* ``limit(num)``: Creates a CassandraRDD with the limit clause applied.
* ``take(num)``: Takes at most ``num`` records from the Cassandra table. Note
that if ``limit()`` was invoked before ``take()`` a normal pyspark ``take()``
is performed. Otherwise, first limit is set and _then_ a ``take()`` is
performed.
* ``cassandraCount()``: Lets Cassandra perform a count, instead of loading the
data to Spark first.
* ``saveToCassandra(...)``: As above, but the keyspace and/or table __may__ be
omitted to save to the same keyspace and/or table.
* ``spanBy(*columns)``: Groups rows by the given columns without shuffling.
* ``joinWithCassandraTable(keyspace, table)``: Join an RDD with a Cassandra
table on the partition key. Use .on(...) to specify other columns to join on.
.select(...), .where(...) and .limit(...) can be used as well.
* ``deleteFromCassandra(keyspace, table, ...)``: Delete rows and columns from
cassandra by implicit `deleteFromCassandra` call


Examples
--------

Creating a SparkContext with Cassandra support

```python
import pyspark_cassandra
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PySpark Cassandra Example") \
    .config("spark.jars", pyspark_cassandra.jar_path()) \
    .config("spark.cassandra.connection.host", "cas-1") \
    .getOrCreate()

sc = spark.sparkContext
```

Using select and where to narrow the data in an RDD and then filter, map,
reduce and collect it:

```python
sc \
    .cassandraTable("keyspace", "table") \
    .select("col-a", "col-b") \
    .where("key=?", "x") \
    .filter(lambda r: r["col-b"].contains("foo")) \
    .map(lambda r: (r["col-a"], 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .collect()
```

Reading from different clusters:

```python
rdd_one = sc \
    .cassandraTable("keyspace", "table_one",
                    connection_config={"spark_cassandra_connection_host": "cas-1"})

rdd_two = sc \
    .cassandraTable("keyspace", "table_two",
                    connection_config={"spark_cassandra_connection_host": "cas-2"})
```

Storing data in Cassandra:

```python
from datetime import timedelta
from random import random

rdd = sc.parallelize([{
    "key": k,
    "stamp": datetime.now(),
    "val": random() * 10,
    "tags": ["a", "b", "c"],
    "options": {
        "foo": "bar",
        "baz": "qux",
    }
} for k in ["x", "y", "z"]])

rdd.saveToCassandra(
    "keyspace",
    "table",
    ttl=timedelta(hours=1),
)

# Storing to non-default cluster
rdd.saveToCassandra(
    "keyspace",
    "table",
    ttl=timedelta(hours=1),
    connection_config={"spark_cassandra_connection_host": "cas-2"}
)
```

Modify CQL collections:

```python
# Cassandra test table schema
# CREATE TABLE test (
#     user_id text, city text, test_set set<text>,
#     test_list list<text>, test_map map<text,text>,
#     PRIMARY KEY (user_id)
# );

rdd = sc.parallelize([{
    "user_id": "123", "city": "berlin",
    "test_set": ["a"], "test_list": ["a"], "test_map": {"a": "1"}
}])

rdd.saveToCassandra("ks", "test")

rdd = sc.parallelize([{
    "user_id": "123", "city": "berlin",
    "test_set": ["a"], "test_list": ["b"], "test_map": {"b": "2"}
}])

rdd.saveToCassandra("ks", "test", {
    "user_id": "", "city": "",
    "test_set": "remove", "test_list": "prepend", "test_map": "append"
})
```

Joining with Cassandra:

```python
joined = rdd \
    .joinWithCassandraTable('keyspace', 'accounts') \
    .on('id') \
    .select('e-mail', 'followers')

for left, right in joined:
    ...
```


Releasing
---------

```console
$ pip install bumpversion

$ bumpversion --dry-run --verbose $CURRENT_VERSION --new-version=$NEW_VERSION

$ bumpversion $CURRENT_VERSION --new-version=$NEW_VERSION

$ git push && git push --tags origin

$ make release-prod
```


Problems / ideas?
------------------
Feel free to use the issue tracker to propose new functionality and / or report
bugs. In case of bugs please provide some code to reproduce the issue or at
least some context information such as software used, CQL schema, etc.

Contributing
-------------

1. Fork it
2. Create your feature branch (git checkout -b my-new-feature)
3. Commit your changes (git commit -am 'Add some feature')
4. Push to the branch (git push origin my-new-feature)
5. Create new Pull Request
