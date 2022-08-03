# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from functools import partial
from pyspark.serializers import AutoBatchedSerializer, PickleSerializer
from pyspark.streaming.dstream import DStream

from pyspark_cassandra.conf import WriteConf, ConnectionConf
from pyspark_cassandra.util import as_java_object, as_java_array
from pyspark_cassandra.util import helper


def saveToCassandra(dstream, keyspace, table, columns=None, row_format=None,
                    keyed=None,
                    write_conf=None, connection_config=None, **write_conf_kwargs):
    ctx = dstream._ssc._sc
    gw = ctx._gateway

    # create write config as map
    write_conf = WriteConf.build(write_conf, **write_conf_kwargs)
    write_conf = as_java_object(gw, write_conf.settings())
    # convert the columns to a string array
    columns = as_java_array(gw, "String", columns) if columns else None

    fn_save = partial(helper(ctx).saveToCassandra, dstream._jdstream, keyspace, table,
                      columns, row_format,
                      keyed, write_conf)

    if connection_config:
        conn_conf = as_java_object(ctx._gateway, ConnectionConf.build(**connection_config).settings())
        fn_save = partial(fn_save, conn_conf)

    return fn_save()


def deleteFromCassandra(dstream, keyspace=None, table=None, deleteColumns=None,
                        keyColumns=None,
                        row_format=None, keyed=None, write_conf=None, connection_config=None,
                        **write_conf_kwargs):
    """Delete data from Cassandra table, using data from the RDD as primary
    keys. Uses the specified column names.

    Arguments:
       @param dstream(DStream)
        The DStream to join. Equals to self when invoking
        joinWithCassandraTable on a monkey patched RDD.
        @param keyspace(string):in
            The keyspace to save the RDD in. If not given and the rdd is a
            CassandraRDD the same keyspace is used.
        @param table(string):
            The CQL table to save the RDD in. If not given and the rdd is a
            CassandraRDD the same table is used.

        Keyword arguments:
        @param deleteColumns(iterable):
            The list of column names to delete, empty ColumnSelector means full
            row.

        @param keyColumns(iterable):
            The list of column names to delete, empty ColumnSelector means full
            row.

        @param row_format(RowFormat):
            Primary key columns selector, Optional. All RDD primary columns
            columns will be checked by default
        @param keyed(bool):
            Make explicit that the RDD consists of key, value tuples (and not
            arrays of length two).

        @param write_conf(WriteConf):
            A WriteConf object to use when saving to Cassandra
        @param **write_conf_kwargs:
            WriteConf parameters to use when saving to Cassandra
    """

    ctx = dstream._ssc._sc
    gw = ctx._gateway

    # create write config as map
    write_conf = WriteConf.build(write_conf, **write_conf_kwargs)
    write_conf = as_java_object(gw, write_conf.settings())
    # convert the columns to a string array
    deleteColumns = as_java_array(gw, "String",
                                  deleteColumns) if deleteColumns else None
    keyColumns = as_java_array(gw, "String", keyColumns) \
        if keyColumns else None
    fn_delete = partial(helper(ctx).deleteFromCassandra, dstream._jdstream, keyspace, table,
                        deleteColumns, keyColumns,
                        row_format,
                        keyed, write_conf)
    if connection_config:
        conn_conf = as_java_object(ctx._gateway, ConnectionConf.build(**connection_config).settings())
        fn_delete = partial(fn_delete, conn_conf)

    return fn_delete()


def joinWithCassandraTable(dstream, keyspace, table, selected_columns=None,
                           join_columns=None, connection_config=None):
    """Joins a DStream (a stream of RDDs) with a Cassandra table

    Arguments:
        @param dstream(DStream)
        The DStream to join. Equals to self when invoking
        joinWithCassandraTable on a monkey patched RDD.
        @param keyspace(string):
            The keyspace to join on.
        @param table(string):
            The CQL table to join on.
        @param selected_columns(string):
            The columns to select from the Cassandra table.
        @param join_columns(string):
            The columns used to join on from the Cassandra table.
    """

    ssc = dstream._ssc
    ctx = ssc._sc
    gw = ctx._gateway

    selected_columns = as_java_array(
        gw, "String", selected_columns) if selected_columns else None
    join_columns = as_java_array(gw, "String",
                                 join_columns) if join_columns else None

    h = helper(ctx)
    fn_read_join = partial(h.joinWithCassandraTable, dstream._jdstream, keyspace, table,
                           selected_columns,
                           join_columns)
    if connection_config:
        conn_conf = as_java_object(ctx._gateway, ConnectionConf.build(**connection_config).settings())
        fn_read_join = partial(fn_read_join, conn_conf)
    dstream = fn_read_join()
    dstream = h.pickleRows(dstream)
    dstream = h.javaDStream(dstream)

    return DStream(dstream, ssc, AutoBatchedSerializer(PickleSerializer()))


# Monkey patch the default python DStream so that data in it can be stored to
# and joined with Cassandra tables
DStream.saveToCassandra = saveToCassandra
DStream.joinWithCassandraTable = joinWithCassandraTable
DStream.deleteFromCassandra = deleteFromCassandra
