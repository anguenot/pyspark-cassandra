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

package pyspark_util

import java.io.{NotSerializableException, OutputStream}
import java.math.BigInteger
import java.net.{Inet4Address, Inet6Address, InetAddress}
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.util.{Collection, HashMap, UUID, Map => JMap, Date}

import net.razorvine.pickle._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import pyspark_util.Conversions._

 import com.datastax.driver.core.LocalDate

import scala.collection.JavaConversions._
import scala.collection.convert.Wrappers.{JListWrapper, JMapWrapper, JSetWrapper}
import scala.collection.immutable.HashMap.HashTrieMap
import scala.collection.immutable.HashSet.{HashSet1, HashTrieSet}
import scala.collection.immutable.Map.{Map1, Map2, Map3, Map4, WithDefault}
import scala.collection.immutable.{Set, Vector}
import scala.collection.mutable.{ArraySeq, Buffer, WrappedArray}

class Pickling extends Serializable {
  register()

  def pickler() = {
    register() // ensure the custom picklers and constructors are registered also after (de)serialization
    new Pickler()
  }

  def unpickler() = {
    register() // ensure the custom picklers and constructors are registered also after (de)serialization
    new Unpickler()
  }

  implicit def toPickleableRDD(rdd: RDD[_]) = new PicklableRDD(rdd)

  implicit def toUnpickleableRDD(rdd: RDD[Array[Byte]]) = new UnpicklableRDD(rdd)

  implicit def toUnpickleableStream(dstream: DStream[Array[Byte]]) = new UnpicklableDStream(dstream)

  def register() {
    Unpickler.registerConstructor("uuid", "UUID", UUIDUnpickler)
//    Unpickler.registerConstructor("datetime", "date", CassandraLocalDateUnpickler)

    Pickler.registerCustomPickler(classOf[UUID], UUIDPickler)
    Pickler.registerCustomPickler(classOf[UUIDHolder], UUIDPickler)
//    Pickler.registerCustomPickler(classOf[org.joda.time.LocalDate], JodaLocalDatePickler) 
//    Pickler.registerCustomPickler(classOf[com.datastax.driver.core.LocalDate], DatastaxLocalDatePickler) 
    Pickler.registerCustomPickler(classOf[InetAddress], AsStringPickler)
    Pickler.registerCustomPickler(classOf[Inet4Address], AsStringPickler)
    Pickler.registerCustomPickler(classOf[Inet6Address], AsStringPickler)
    Pickler.registerCustomPickler(classOf[ByteBuffer], ByteBufferPickler)
    Pickler.registerCustomPickler(Class.forName("java.nio.HeapByteBuffer"), ByteBufferPickler)
    Pickler.registerCustomPickler(classOf[GatheredByteBuffers], GatheringByteBufferPickler)
    Pickler.registerCustomPickler(Class.forName("scala.collection.immutable.$colon$colon"), ListPickler)
    Pickler.registerCustomPickler(classOf[List[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[ArraySeq[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[Buffer[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[WrappedArray.ofRef[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[JListWrapper[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[JSetWrapper[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[Stream.Cons[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple1[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple2[_, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple3[_, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple4[_, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple5[_, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple6[_, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple7[_, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple8[_, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple9[_, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple10[_, _, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple11[_, _, _, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple12[_, _, _, _, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple13[_, _, _, _, _, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple14[_, _, _, _, _, _, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], ListPickler)
    Pickler.registerCustomPickler(classOf[Vector[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[Set[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[Set.Set1[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[Set.Set2[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[Set.Set3[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[Set.Set4[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[HashSet1[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[HashTrieSet[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[Map[_, _]], MapPickler)
    Pickler.registerCustomPickler(classOf[WithDefault[_, _]], MapPickler)
    Pickler.registerCustomPickler(classOf[Map1[_, _]], MapPickler)
    Pickler.registerCustomPickler(classOf[Map2[_, _]], MapPickler)
    Pickler.registerCustomPickler(classOf[Map3[_, _]], MapPickler)
    Pickler.registerCustomPickler(classOf[Map4[_, _]], MapPickler)
    Pickler.registerCustomPickler(classOf[HashTrieMap[_, _]], MapPickler)
    Pickler.registerCustomPickler(classOf[JMapWrapper[_, _]], MapPickler)
  }
}

object Pickling extends Pickling

class PicklableRDD(rdd: RDD[_]) {
  def pickle()(implicit pickling: Pickling) = rdd.mapPartitions(new BatchPickler(), true)
}

class UnpicklableRDD(rdd: RDD[Array[Byte]]) {
  def unpickle()(implicit pickling: Pickling) = rdd.flatMap(new BatchUnpickler())
}

class UnpicklableDStream(dstream: DStream[Array[Byte]]) {
  def unpickle()(implicit pickling: Pickling) = dstream.flatMap(new BatchUnpickler())
}

class BatchPickler(batchSize: Int = 1000)(implicit pickling: Pickling)
  extends (Iterator[_] => Iterator[Array[Byte]])
    with Serializable {

  def apply(in: Iterator[_]): Iterator[Array[Byte]] = {
    in.grouped(batchSize).map { b => pickling.pickler().dumps(b.toArray) }
  }
}

class BatchUnpickler(implicit pickling: Pickling) extends (Array[Byte] => Seq[Any]) with Serializable {
  def apply(in: Array[Byte]): Seq[Any] = {
    val unpickled = pickling.unpickler().loads(in)
    asSeq(unpickled)
  }
}

trait StructPickler extends IObjectPickler {
  def pickle(o: Any, out: OutputStream, pickler: Pickler): Unit = {
    out.write(Opcodes.GLOBAL)
    out.write(creator.getBytes())
    out.write(Opcodes.MARK)
    val f = fields(o)
    pickler.save(f)
    pickler.save(values(o, f))
    out.write(Opcodes.TUPLE)
    out.write(Opcodes.REDUCE)
  }

  def creator: String

  def fields(o: Any): Seq[_]

  def values(o: Any, fields: Seq[_]): Seq[_]
}

trait StructUnpickler extends IObjectConstructor {
  def construct(args: Array[AnyRef]): Object = {
    val fields = asSeq[String](args(0))
    val values = asSeq[AnyRef](args(1))

    construct(fields, values)
  }

  def construct(fields: Seq[String], values: Seq[AnyRef]): Object
}

object AsStringPickler extends IObjectPickler {
  def pickle(o: Any, out: OutputStream, pickler: Pickler) = pickler.save(o.toString())
}

object DatastaxLocalDatePickler extends IObjectPickler {
  def pickle(o: Any, out: OutputStream, pickler: Pickler): Unit = {
    out.write(Opcodes.GLOBAL);
    out.write("datetime\ndate\n".getBytes());
    // python itself uses the constructor with a single timestamp byte string of len 4,
    // we take the easy way out and just provide 3 ints (year/month/day)
    val date = o.asInstanceOf[com.datastax.driver.core.LocalDate]
    pickler.save(date.getYear());
    pickler.save(date.getMonth());    // months start at 0 in java
    pickler.save(date.getDay());
    out.write(Opcodes.TUPLE3);
    out.write(Opcodes.REDUCE);
  }
}

object JodaLocalDatePickler extends IObjectPickler {
  def pickle(o: Any, out: OutputStream, pickler: Pickler): Unit = {
    out.write(Opcodes.GLOBAL);
    out.write("datetime\ndate\n".getBytes());
    // python itself uses the constructor with a single timestamp byte string of len 4,
    // we take the easy way out and just provide 3 ints (year/month/day)
    val date = o.asInstanceOf[org.joda.time.LocalDate]
    pickler.save(date.getYear());
    pickler.save(date.getMonthOfYear());    // months start at 0 in java
    pickler.save(date.getDayOfMonth());
    out.write(Opcodes.TUPLE3);
    out.write(Opcodes.REDUCE);
  }
}

object CassandraLocalDateUnpickler extends IObjectConstructor {
  def construct(args: Array[Object]): LocalDate = {
    args.size match {
      case 3 => {
        val year = args(0).asInstanceOf[Int]
        val month = args(1).asInstanceOf[Int]
        val day = args(2).asInstanceOf[Int]
        LocalDate.fromYearMonthDay(year, month, day)
      }
      case 1 => args(0) match {
        case params: String => {
          if (params.size != 4)
            throw new PickleException("invalid pickle data for date; expected arg of length 4, got length "+params.size)

          val yhi = params(0)
          val ylo = params(1)
          val month = params(2)
          val day = params(3)
          LocalDate.fromYearMonthDay(yhi * 256 + ylo, month, day)
        }
        case _ => {
          val params = args(0).asInstanceOf[Array[Byte]]
          if (params.size != 4)
            throw new PickleException("invalid pickle data for date; expected arg of length 4, got length "+params.size)
          val yhi = params(0)&0xff
          val ylo = params(1)&0xff
          val month = (params(2)&0xff) // blargh: months start at 0 in java
          val day = params(3)&0xff
          val date = LocalDate.fromYearMonthDay(yhi * 256 + ylo, month, day)
          date
        }
      }
      case _ => throw new PickleException("invalid pickle data for date; expected 1 arg, got "+args.size)
    }
  }
}


object UUIDPickler extends IObjectPickler {
  def pickle(o: Any, out: OutputStream, pickler: Pickler): Unit = {
    out.write(Opcodes.GLOBAL)
    out.write("uuid\nUUID\n".getBytes())
    out.write(Opcodes.MARK)
    o match {
      case uuid: UUID => pickler.save(uuid.toString())
      case holder: UUIDHolder => pickler.save(holder.uuid.toString())
    }
    out.write(Opcodes.TUPLE)
    out.write(Opcodes.REDUCE)
  }
}

object UUIDUnpickler extends IObjectConstructor {
  def construct(args: Array[Object]): Object = {
    args.size match {
      case 1 => UUID.fromString(args(0).asInstanceOf[String])
      case _ => new UUIDHolder()
    }
  }
}

class UUIDHolder {
  var uuid: UUID = null

  def __setstate__(values: HashMap[String, Object]): UUID = {
    val i = values.get("int").asInstanceOf[BigInteger]
    val buffer = ByteBuffer.wrap(i.toByteArray())
    uuid = new UUID(buffer.getLong(), buffer.getLong())
    uuid
  }
}

class GatheredByteBuffers(buffers: Seq[ByteBuffer]) extends Iterable[ByteBuffer] {
  def iterator() = buffers.iterator
}

class GatheringByteBufferPickler extends IObjectPickler {
  def pickle(o: Any, out: OutputStream, currentPickler: Pickler) = {
    val buffers = o.asInstanceOf[GatheredByteBuffers]

    out.write(Opcodes.GLOBAL)
    out.write("__builtin__\nbytearray\n".getBytes())

    out.write(Opcodes.BINSTRING)

    val length = buffers.map {
      _.remaining()
    }.sum

    out.write(PickleUtils.integer_to_bytes(length))

    val c = Channels.newChannel(out)
    buffers.foreach {
      c.write(_)
    }

    out.write(Opcodes.TUPLE1)
    out.write(Opcodes.REDUCE)
  }
}

object GatheringByteBufferPickler extends GatheringByteBufferPickler

object ByteBufferPickler extends GatheringByteBufferPickler {
  override def pickle(o: Any, out: OutputStream, pickler: Pickler) = {
    val buffers = new GatheredByteBuffers(o.asInstanceOf[ByteBuffer] :: Nil)
    super.pickle(buffers, out, pickler)
  }
}

object ListPickler extends IObjectPickler {
  def pickle(o: Any, out: OutputStream, pickler: Pickler): Unit = {
    pickler.save(
      o match {
        case c: Collection[_] => c
        case b: Buffer[_] => bufferAsJavaList(b)
        case s: Seq[_] => seqAsJavaList(s)
        case p: Product => seqAsJavaList(p.productIterator.toSeq)
        case s: Set[_] => setAsJavaSet(s)
        case _ => throw new NotSerializableException(o.toString())
      })
  }
}

object MapPickler extends IObjectPickler {
  def pickle(o: Any, out: OutputStream, pickler: Pickler): Unit = {
    pickler.save(
      o match {
        case m: JMap[_, _] => m
        case m: Map[_, _] => mapAsJavaMap(m)
      })
  }
}
