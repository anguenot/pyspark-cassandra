PySpark Cassandra
=================

[![Build
Status](https://travis-ci.org/anguenot/pyspark-cassandra.svg)](https://travis-ci.org/anguenot/pyspark-cassandra)
[![APACHE2
License](https://img.shields.io/badge/license-Apache2.0-blue.svg?style=flat-square)](https://opensource.org/licenses/Apache-2.0)

`pyspark-cassandra` is a Python port of the awesome [DataStax Cassandra
Connector](https://github.com/datastax/spark-cassandra-connector).

This module provides Python support for Apache Spark's Resilient Distributed
Datasets from Apache Cassandra CQL rows using [Cassandra Spark
Connector](https://github.com/datastax/spark-cassandra-connector) within
PySpark, both in the interactive shell and in Python programs submitted with
spark-submit.

This project was initially forked from
[@TargetHolding](https://github.com/TargetHolding/pyspark-cassandra) since they
no longer maintain it.

**Contents:**
* [Compatibility](#compatibility)
* [Using with PySpark](#using-with-pyspark)
* [Using with PySpark shell](#using-with-pyspark-shell)
* [Building](#building)
* [API](#api)
* [Examples](#examples)
* [Problems / ideas?](#problems--ideas)
* [Contributing](#contributing)


Compatibility
-------------
Feedback on (in-)compatibility are much appreciated.

### Spark
The current version of PySpark Cassandra is successfully used with Spark version
2.3.x.

For Spark versions 2.0.x, 2.1.x, 2.2.x: use version 0.9.0

for Spark 1.5.x, 1.6.x use [older versions](https://github.com/TargetHolding/pyspark-cassandra) 

### Cassandra
PySpark Cassandra is compatible with Cassandra:
* 2.1.5*
* 2.2.x
* 3.0.x
* 3.11.x

### Python
PySpark Cassandra is used with python 2.7, python 3.4+

### Scala
PySpark Cassandra is currently only packaged for Scala 2.11

Using with PySpark
------------------

### With Spark Packages
Pyspark Cassandra is published at [Spark
Packages](http://spark-packages.org/package/anguenot/pyspark-cassandra). This
allows easy usage with Spark through:
```bash
spark-submit \
	--packages anguenot/pyspark-cassandra:<version> \
	--conf spark.cassandra.connection.host=your,cassandra,node,names
```


### Without Spark Packages

```bash
spark-submit \
	--jars /path/to/pyspark-cassandra-assembly-<version>.jar \
	--py-files /path/to/pyspark-cassandra-assembly-<version>.jar \
	--conf spark.cassandra.connection.host=your,cassandra,node,names \
	--master spark://spark-master:7077 \
	yourscript.py
```
(also not that the assembly will include the python source files, quite similar 
to a python source distribution)


Using with PySpark shell
------------------------

Replace `spark-submit` with `pyspark` to start the interactive shell and don't 
provide a script as argument and then import PySpark Cassandra. Note that when 
performing this import the `sc` variable in pyspark is augmented with 
the `cassandraTable(...)` method.

```python
import pyspark_cassandra
```


Building
--------

### For [Spark Packages](http://spark-packages.org/package/anguenot/pyspark-cassandra) Pyspark Cassandra can be published using:
```bash
sbt compile
```
The package can be published locally with:
```bash
sbt spPublishLocal
```
The package can be published to Spark Packages with (requires authentication and
 authorization):
```bash
make publish
```

### For local testing / without Spark Packages
A Java / JVM library as well as a python library is required to use PySpark 
Cassandra. They can be built with:

```bash
make dist
```

This creates a fat jar with the Spark Cassandra Connector and additional classes 
for bridging Spark and PySpark for Cassandra data and the .py source files at: 
`target/scala-2.11/pyspark-cassandra-assembly-<version>.jar`



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
* `DICT`: The default layout, a CQL row is represented as a python dict with the
 CQL row columns as keys.
* `TUPLE`: A CQL row is represented as a python tuple with the values in CQL 
table column order / the order of the selected columns.
* `ROW`: A pyspark_cassandra.Row object representing a CQL row.

Column values are related between CQL and python as follows:

|  **CQL**  |       **python**      |
|:---------:|:---------------------:|
|   ascii   |    unicode string     |
|   bigint  |         long          |
|    blob   |       bytearray       |
|  boolean  |        boolean        |
|  counter  |       int, long       |
|  decimal  |        decimal        |
|   double  |         float         |
|   float   |         float         |
|    inet   |          str          |
|    int    |          int          |
|    map    |         dict          |
|    set    |          set          |
|    list   |         list          |
|    text   |    unicode string     |
| timestamp |   datetime.datetime   |
|  timeuuid |       uuid.UUID       |
|  varchar  |    unicode string     |
|   varint  |         long          |
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


### pyspark_cassandra.streaming

When importing ```pyspark_cassandra.streaming``` the method 
``saveToCassandra(...)``` is made available on DStreams. Also support for 
joining with a Cassandra table is added:
* ``joinWithCassandraTable(keyspace, table, selected_columns, join_columns)``: 
 Join an RDD with a Cassandra table on the partition key. Use .on(...) to
 specify other columns to join on. .select(...), .where(...) and .limit(...) can
  be used as well.
* ``deleteFromCassandra(keyspace, table, ...)``: Delete rows and columns from 
cassandra by implicit `deleteFromCassandra` call


Examples
--------

Creating a SparkContext with Cassandra support

```python
import pyspark_cassandra

conf = SparkConf() \
	.setAppName("PySpark Cassandra Test") \
	.setMaster("spark://spark-master:7077") \
	.set("spark.cassandra.connection.host", "cas-1")

sc = CassandraSparkContext(conf=conf)
```

Using select and where to narrow the data in an RDD and then filter, map, 
reduce and collect it::

```python
sc \
	.cassandraTable("keyspace", "table") \
	.select("col-a", "col-b") \
	.where("key=?", "x") \
	.filter(lambda r: r["col-b"].contains("foo")) \
	.map(lambda r: (r["col-a"], 1)
	.reduceByKey(lambda a, b: a + b)
	.collect()
```

Storing data in Cassandra::

```python
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
```

Modify CQL collections::

```python

# Cassandra test table schema
# create table test (user_id text, city text,  test_set set<text>, test_list list<text>, test_map map<text,text>, PRIMARY KEY (user_id));

rdd = sc.parallelize([{"user_id":"123","city":"berlin","test_set":["a"],"test_list":["a"],"test_map":{"a":"1"}}])

rdd.saveToCassandra("ks","test")

rdd = sc.parallelize([{"user_id":"123","city":"berlin","test_set":["a"],"test_list":["b"],"test_map":{"b":"2"}}])

rdd.saveToCassandra("ks","test", {"user_id":"", "city":"", "test_set":"remove", "test_list":"prepend", "test_map":"append"})

```

Create a streaming context, convert every line to a generater of words which 
are saved to cassandra. Through this example all unique words are stored in 
Cassandra.

The words are wrapped as a tuple so that they are in a format which can be 
stored. A dict or a pyspark_cassandra.Row object would have worked as well.

```python
from pyspark.streaming import StreamingContext
from pyspark_cassandra import streaming

ssc = StreamingContext(sc, 2)

ssc \
    .socketTextStream("localhost", 9999) \
    .flatMap(lambda l: ((w,) for w in (l,))) \
    .saveToCassandra('keyspace', 'words')

ssc.start()
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

Or with a DStream:

```python
joined = dstream.joinWithCassandraTable(self.keyspace, self.table, \
    ['e-mail', 'followers'], ['id'])
```

Releasing
---------

```console

$ pip install bumpversion

$ bumpversion --dry-run --verbose $CURRENT_VERSION --new-version=$NEW_VERSION

$ bumpversion $CURRENT_VERSION --new-version=$NEW_VERSION

$ git push

$ git push --tags origin

```

Problems / ideas?
------------------
Feel free to use the issue tracker propose new functionality and / or report 
bugs. In case of bugs please provides some code to reproduce the issue or at 
least some context information such as software used, CQL schema, etc.

Contributing
-------------

1. Fork it
2. Create your feature branch (git checkout -b my-new-feature)
3. Commit your changes (git commit -am 'Add some feature')
4. Push to the branch (git push origin my-new-feature)
5. Create new Pull Request
