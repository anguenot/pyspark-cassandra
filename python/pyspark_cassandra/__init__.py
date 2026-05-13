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

"""
    This module provides python support for Apache Spark's Resillient
    Distributed Datasets from Apache Cassandra CQL rows using the Spark
    Cassandra Connector from:
    https://github.com/datastax/spark-cassandra-connector.
"""

import glob
import inspect
import os

import pyspark.rdd

from .conf import ReadConf, WriteConf
from .context import CassandraSparkContext, monkey_patch_sc
from .rdd import RowFormat
from .rdd import deleteFromCassandra, joinWithCassandraTable, saveToCassandra
from .types import Row, UDT

__all__ = [
    "CassandraSparkContext",
    "ReadConf",
    "Row",
    "RowFormat",
    "UDT",
    "WriteConf",
    "jar_path",
]


def jar_path():
    """Returns the path to the bundled assembly JAR, or None if not found."""
    jars_dir = os.path.join(os.path.dirname(__file__), 'jars')
    jars = glob.glob(os.path.join(jars_dir, '*.jar'))
    return jars[0] if jars else None


# Monkey patch the default python RDD so that it can be stored to Cassandra as
# CQL rows

pyspark.rdd.RDD.saveToCassandra = saveToCassandra
pyspark.rdd.RDD.joinWithCassandraTable = joinWithCassandraTable
pyspark.rdd.RDD.deleteFromCassandra = deleteFromCassandra

# Auto-add the bundled JAR to an active SparkContext if one exists
_jar = jar_path()
if _jar:
    from pyspark import SparkContext
    _sc = SparkContext._active_spark_context
    if _sc is not None:
        _sc.addJar(_jar)

# Monkey patch the sc variable in the caller if any
frame = inspect.currentframe().f_back
# Go back at most 10 frames
for _ in range(10):
    if not frame:
        break
    elif "sc" in frame.f_globals:
        monkey_patch_sc(frame.f_globals["sc"])
        break
    else:
        frame = frame.f_back

__author__ = 'Julien Anguenot'
__email__ = 'julien@anguenot.org'
__version__ = '3.5.0'
