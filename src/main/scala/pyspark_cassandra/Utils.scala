/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pyspark_cassandra

import java.util.{Map => JMap}

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._
import com.datastax.spark.connector.writer._
import org.apache.spark.SparkContext

import scala.jdk.CollectionConverters._

object Utils {

  def columnSelector(columns: JMap[String, String]) = {

    if (columns != null && !columns.isEmpty()) {
      SomeColumns(columns.asScala.toList.map(entry => (entry._1, entry._2) match {
        case (key, "append") => CollectionColumnName(key, None, CollectionAppend)
        case (key, "add") => CollectionColumnName(key, None, CollectionAppend)
        case (key, "overwrite") => CollectionColumnName(key, None, CollectionOverwrite)
        case (key, "prepend") => CollectionColumnName(key, None, CollectionPrepend)
        case (key, "remove") => CollectionColumnName(key, None, CollectionRemove)
        case (key, "") => ColumnName(key)
        case (key, _) => throw new IllegalArgumentException("The only possible values are: " +
          "append/add/overwrite/prepend/remove or empty")
      }): _*)

    } else {
      AllColumns
    }
  }

  def columnSelector(columns: Array[String], default: ColumnSelector = AllColumns) = {
    if (columns != null && columns.length > 0) {
      SomeColumns(columns.map {
        ColumnName(_)
      }: _*)
    } else {
      default
    }
  }

  def parseReadConf(sc: SparkContext, readConf: Option[JMap[String, Any]]) = {
    var conf = ReadConf.fromSparkConf(sc.getConf)

    readConf match {
      case Some(rc) =>
        for {(k, v) <- rc.asScala} {
          (k, v) match {
            case ("split_count", v: Int) => conf = conf.copy(splitCount = Option(v))
            case ("split_size", v: Int) => conf = conf.copy(splitSizeInMB = v)
            case ("fetch_size", v: Int) => conf = conf.copy(fetchSizeInRows = v)
            case ("consistency_level", v: Int) => conf = conf.copy(consistencyLevel = DefaultConsistencyLevel.values()(v))
            case ("consistency_level", v) => conf = conf.copy(consistencyLevel = DefaultConsistencyLevel.valueOf(v.toString))
            case ("metrics_enabled", v: Boolean) => conf = conf.copy(taskMetricsEnabled = v)
            case _ => throw new IllegalArgumentException(s"Read conf key $k with value $v unsupported")
          }
        }
      case None => // do nothing
    }

    conf
  }

  def parseWriteConf(writeConf: Option[JMap[String, Any]]) = {
    var conf = WriteConf()

    writeConf match {
      case Some(wc) =>
        for {(k, v) <- wc.asScala} {
          (k, v) match {
            case ("batch_size", v: Int) => conf = conf.copy(batchSize = BytesInBatch(v))
            case ("batch_buffer_size", v: Int) => conf = conf.copy(batchGroupingBufferSize = v)
            case ("batch_grouping_key", "replica_set") => conf = conf.copy(batchGroupingKey = BatchGroupingKey.ReplicaSet)
            case ("batch_grouping_key", "partition") => conf = conf.copy(batchGroupingKey = BatchGroupingKey.ReplicaSet)
            case ("consistency_level", v: Int) => conf = conf.copy(consistencyLevel = DefaultConsistencyLevel.values()(v))
            case ("consistency_level", v) => conf = conf.copy(consistencyLevel = DefaultConsistencyLevel.valueOf(v.toString))
            case ("parallelism_level", v: Int) => conf = conf.copy(parallelismLevel = v)
            case ("throughput_mibps", v: Number) => conf = conf.copy(throughputMiBPS = Some(v.doubleValue()))
            case ("ttl", v: Int) => conf = conf.copy(ttl = TTLOption.constant(v))
            case ("ttl", v: String) => conf = conf.copy(ttl = TTLOption.perRow(v))
            case ("timestamp", v: Number) => conf = conf.copy(timestamp = TimestampOption.constant(v.longValue()))
            case ("metrics_enabled", v: Boolean) => conf = conf.copy(taskMetricsEnabled = v)
            case ("ignore_nulls", v: Boolean) => conf = conf.copy(ignoreNulls = v)
            case _ => throw new IllegalArgumentException(s"Write conf key $k with value $v unsupported")
          }
        }
      case None => // do nothing
    }

    conf
  }
}

object Format extends Enumeration {
  val DICT, TUPLE, ROW = Value

  def apply(format: Integer): Option[Format.Value] =
    if (format != null) Some(Format(format)) else None

  def parser(example: Any): FromUnreadRow[_] = {
    val format = detect(example)
    return Format.parser(format._1, format._2)
  }

  def parser(format: Option[Format.Value], keyed: Option[Boolean]): FromUnreadRow[_] =
    parser(format.getOrElse(Format.ROW), keyed.getOrElse(false))

  def parser(format: Format.Value, keyed: Boolean): FromUnreadRow[_] = {
    (format, keyed) match {
      case (Format.ROW, false) => ToRow
      case (Format.ROW, true) => ToKVRows

      case (Format.TUPLE, false) => ToTuple
      case (Format.TUPLE, true) => ToKVTuple

      case (Format.DICT, false) => ToDict
      case (Format.DICT, true) => ToKVDicts

      case _ => throw new IllegalArgumentException()
    }
  }

  def detect(row: Any) = {
    row match {
      case row: Row => (Format.ROW, false)

      case row: Map[_, _] => (Format.DICT, false)
      case row: JMap[_, _] => (Format.DICT, false)

      case row: Array[_] =>
        if (row.length == 2) {
          val Array(k, v) = row

          k match {
            case k: Map[_, _] => (Format.DICT, true)
            case k: Array[_] => (Format.TUPLE, true)
            case k: Row => (Format.ROW, true)
            case _ => (Format.TUPLE, false)
          }
        }

        (Format.TUPLE, false)

      case _ =>
        val cls = row.getClass()
        throw new RuntimeException(s"Unable to detect or unsupported row format ($cls). Set it explicitly " +
          "with saveToCassandra(..., row_format=...).")
    }
  }
}
