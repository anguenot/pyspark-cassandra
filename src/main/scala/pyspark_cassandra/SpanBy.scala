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

import com.datastax.oss.driver.api.core.`type`.{DataType, DataTypes}
import com.datastax.spark.connector.{GettableData, toRDDFunctions}
import org.apache.spark.rdd.RDD

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ArrayBuffer

case class DataFrame(names: Array[String], types: Array[String], values: Seq[ArrayBuffer[Any]])

object SpanBy {
  def binary(rdd: RDD[UnreadRow], columns: Array[String]) = {
    val spanned = rdd.spanBy { r => columns.map { c => r.row.getBytesUnsafe(c) } }

    spanned.map {
      case (k, rows) => {
        val colDefs = rows.head.row.getColumnDefinitions.asScala.toSeq
        val colTypesWithIdx = colDefs.map {
          d => d.getType
        }.zipWithIndex.filter {
          case (c, i) => columns.contains(colDefs(i).getName.asCql(true))
        }

        val deserializedKey = k.zipWithIndex.map {
          case (bb, i) => GettableData.get(rows.head.row, i)
        }

        val df = colDefs.map { x => new ArrayBuffer[Any] }
        for {
          row <- rows
          (ct, i) <- colTypesWithIdx
        } {
          df(i) += deserialize(row, ct, i)
        }

        val numpyTypes = colTypesWithIdx.map { case (c, i) => numpyType(c).getOrElse(null) }

        (deserializedKey, new DataFrame(columns, numpyTypes.toArray, df))
      }
    }
  }

  private def deserialize(row: UnreadRow, dt: DataType, i: Int) = {
    if (binarySupport(dt)) {
      row.row.getBytesUnsafe(i)
    } else {
      GettableData.get(row.row, i)
    }
  }

  private def binarySupport(dataType: DataType) = {
    numpyType(dataType) match {
      case Some(x) => true
      case None => false
    }
  }

  private def numpyType(dataType: DataType) = {
    Option(dataType match {
      case DataTypes.BOOLEAN => ">b1"
      case DataTypes.INT => ">i4"
      case DataTypes.BIGINT => ">i8"
      case DataTypes.COUNTER => ">i8"
      case DataTypes.FLOAT => ">f4"
      case DataTypes.DOUBLE => ">f8"
      case DataTypes.TIMESTAMP => ">M8[ms]"
      case _ => null
    })
  }
}
