=======
CHANGES
=======

2.4.0 (2018-08-06)
------------------
* Python3 support for development / tests. Deprecate Python 2.7
* Support for Spark 2.4.x

0.11.0 (2019-07-12)
-------------------

* integration tests running against Spark 2.3.3
* integration tests running against Cassandra 3.11.4, 3.0.18, 2.2.14
* [#44] drop C* 2.1 support by removing it from the tests matrix.
* [#19][#21] date type support

0.10.1 (2019-02-06)
-------------------

* build against Spark 2.3.2 (see https://github.com/datastax/spark-cassandra-connector)
* deprecate Spark 2.0, 2.1 and 2.2
* integration tests running against Spark 2.3.2
* integration tests running against Cassandra 3.11.3, 3.0.17, 2.2.13, 2.1.20

0.10.0 (2019-02-06)
-------------------

* build against Spark 2.3.1
* integration tests running against Spark 2.3.0
* avoid exceptions for empty lists
* fix syntax error in `requirements_dev.txt`

0.9.0 (2018-06-08)
------------------

* Implementation of append/add/overwrite/prepend/remove as a dictionary in python interface

0.8.0 (2018-05-31)
------------------

* spark-cassandra-connector 2.3.0
* integration tests running against Spark 2.2.1, Spark 2.3.0
* build against Spark 2.3.0
* integration tests running against Cassandra 3.11.2, 3.0.16, 2.2.12, 2.1.20

0.7.0 (2017-12-12)
------------------

* spark-cassandra-connector 2.0.6
* integration tests running against Spark 2.1.2

0.6.0 (2017-10-05)
------------------

* make PythonHelper.scala serializable
* integration tests running against C* 2.1.18, 2.2.10, 3.0.14 and 3.11.0
* spark-cassandra-connector 2.0.5
* build against Spark 2.2.0

0.5.0 (2017-06-19)
------------------

* cleanups: build, imports, format etc.
* expose `deleteFromCassandra()` binding on `pyspark_cassandra.CassandraRDD` and `pyspark_cassandra.streaming`

0.4.0 (2017-06-09)
------------------

* Fork base project and support for Spark 2.0 / 2.1, Scala 2.11 and
  spark-cassandra-connector 2.0.2


