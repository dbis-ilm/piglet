/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dbis.piglet.backends.spark

import dbis.piglet.backends._
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.tools.nsc.io._

//-----------------------------------------------------------------------------------------------------

class PigStorage[T <: SchemaClass :ClassTag](fraction: Int = 1) extends java.io.Serializable {
  
  def load(sc: SparkContext, path: String, extract: (Array[String]) => T, delim: String = "\t", 
      skipFirstRow: Boolean = false, skipEmpty: Boolean = false, comments: String = "", lineageAndAccum: Option[(String, SizeAccumulator2)] = None): RDD[T] = {
        
    val raw = sc.textFile(path)
    val nonEmpty = if(skipEmpty) raw.filter { line => line.nonEmpty } else raw
    val nonComment = if(comments.nonEmpty) nonEmpty.filter { line => !line.startsWith(comments) } else nonEmpty
    val content = if(skipFirstRow) {
      val header = nonComment.first()
      nonComment.filter { line => line != header }
    } else 
      nonComment



    val rawArr = content.map(line => line.split(delim, -1))

    if(lineageAndAccum.isDefined) {
      val (lineage, accum) = lineageAndAccum.get
      rawArr.map{ arr =>
        val t = extract(arr)
        if(scala.util.Random.nextInt(fraction) == 0) {
//          accum.incr(lineage, t.getNumBytes)
            accum.incr(lineage, PerfMonitor.estimateSize(t))
        }
        t
      }
    } else
      rawArr.map(extract)
  }

  def write(path: String, rdd: RDD[T], delim: String = ",") = rdd.map(_.mkString(delim)).saveAsTextFile(path)
}

object PigStorage extends java.io.Serializable {
  def apply[T <: SchemaClass: ClassTag](fraction: Int = 1): PigStorage[T] = {
    new PigStorage[T](fraction)
  }
}

//-----------------------------------------------------------------------------------------------------

class TextLoader[T <: SchemaClass :ClassTag](franction: Int = -1) extends java.io.Serializable {
  def load(sc: SparkContext, path: String, extract: (Array[String]) => T, lineageAndAccum: Option[(String, SizeAccumulator2)] = None): RDD[T] = {
    sc.textFile(path).map(line => extract(Array(line)))  //.map(line => Record(Array(line)))
  }


  def loadStream(ssc: StreamingContext, path: String, extract: (Array[String]) => T): DStream[T] =
    ssc.queueStream(mutable.Queue(ssc.sparkContext.textFile(path)),oneAtATime = true, null).map(line => extract(Array(line)))
    //ssc.readFile(path).map(line => extract(Array(line)))
}

object TextLoader extends java.io.Serializable {
  def apply[T <: SchemaClass: ClassTag](fraction: Int = -1): TextLoader[T] = {
    new TextLoader[T](fraction)
  }
}

//-----------------------------------------------------------------------------------------------------

class PigStream[T <: SchemaClass: ClassTag] extends java.io.Serializable {
  def receiveStream(ssc: StreamingContext, hostname: String, port: Int, extract: (Array[String]) => T, delim: String = "\t"): DStream[T] =
    ssc.socketTextStream(hostname, port).map(line => extract(line.split(delim, -1)))

  def loadStream(ssc: StreamingContext, path: String, extract: (Array[String]) => T, delim: String = "\t"): DStream[T] ={
     ssc.queueStream(mutable.Queue(ssc.sparkContext.textFile(path)),oneAtATime = true, null).map(line => extract(line.split(delim, -1)))
  }
    //ssc.readFile(path).map(line => extract(Array(line)))

  def writeStream(path: String, dstream: DStream[T], delim: String = ",") =
    dstream.foreachRDD(rdd => rdd.foreach { t => Path(path).createFile().appendAll(t.mkString(delim) + "\r\n") })

}

object PigStream extends java.io.Serializable {
  def apply[T <: SchemaClass: ClassTag](): PigStream[T] = {
    new PigStream[T]
  }
}

//-----------------------------------------------------------------------------------------------------

class RDFFileStorage[T: ClassTag](fraction: Int = -1) extends java.io.Serializable {
  val pattern = "([^\"]\\S*|\".+?\")\\s*".r

  // TODO: use a real RDF library (Jena, raptor) to read RDF files
  def rdfize(line: String): Array[String] = {
    val fields = pattern.findAllIn(line).map(_.trim)
    fields.toArray.slice(0, 3)
  }

  def load(sc: SparkContext, path: String, extract: (Array[String]) => T, lineageAndAccum: Option[(String, SizeAccumulator2)] = None): RDD[T] =
    sc.textFile(path).map(line => extract(rdfize(line)))
}

object RDFFileStorage {
  def apply[T: ClassTag](fraction: Int = -1): RDFFileStorage[T] = {
    new RDFFileStorage[T](fraction )
  }
}

//-----------------------------------------------------------------------------------------------------

class BinStorage[T: ClassTag](fraction: Int = -1) extends java.io.Serializable {

  def load(sc: SparkContext, path: String, extract: (Any) => Any = (any) => any, lineageAndAccum: Option[(String, SizeAccumulator2)] = None): RDD[T] = sc.objectFile[T](path)
//  def load(sc: SparkContext, path: String, lineageAndAccum: Option[(String, SizeAccumulator2)]): RDD[T] =

  def write(path: String, rdd: RDD[T]) = rdd.saveAsObjectFile(path)
}

object BinStorage {
  def apply[T: ClassTag](fraction: Int = -1): BinStorage[T] = new BinStorage[T](fraction)
}

import org.json4s._
import org.json4s.native.Serialization


class JsonStorage2[T <: SchemaClass : Manifest](fraction: Int = -1)  extends Serializable {

  def load(sc: SparkContext, path: String, extract: (Any) => Any = (any) => any, lineageAndAccum: Option[(String, SizeAccumulator2)] = None): RDD[T] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    import spark.implicits._
    val a = spark.read.json(path).as[T]
    a.rdd.map{ t =>
      if (lineageAndAccum.isDefined) {
        val (lineage, accum) = lineageAndAccum.get

        if (scala.util.Random.nextInt(fraction) == 0) {
          accum.incr(lineage, PerfMonitor.estimateSize(t))
        }
      }
      t
    }
  }


  def write(path: String, rdd: RDD[T]) = {
    val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
    import spark.sqlContext.implicits._
    rdd.toDF().write.json(path)
  }
}

object JsonStorage2 {
  def apply[T <: SchemaClass: Manifest](fraction: Int = -1): JsonStorage2[T] = new JsonStorage2(fraction)
}

//-----------------------------------------------------------------------------------------------------

class JsonStorage(fraction: Int = -1) extends java.io.Serializable {
  def load(sc: SparkContext, path: String, lineageAndAccum: Option[(String, SizeAccumulator2)] = None): RDD[List[Any]] = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // TODO: convert a DataFrame to a RDD with generic components
    sqlContext.read.json(path).rdd.map(_.toSeq.map(v => v.toString).toList)
  }
}

object JsonStorage {
  def apply(fraction: Int = -1): JsonStorage = new JsonStorage(fraction)
}

//-----------------------------------------------------------------------------------------------------

class JdbcStorage[T <: SchemaClass: ClassTag](fraction: Int = -1) extends java.io.Serializable {
  def load(sc: SparkContext, table: String, extract: Row => T, driver: String, url: String, lineageAndAccum: Option[(String, SizeAccumulator2)] = None): RDD[T] = {
    // sc.addJar("/Users/kai/Projects/h2/bin/h2-1.4.189.jar")
    var params = scala.collection.immutable.Map[String, String]()
    params += ("driver" -> driver)
    params += ("dbtable" -> table)
    val s1 = url.split("""\?""")
    params += ("url" -> s1(0))
    if (s1.length > 1) {
      val s2 = s1(1).split("&")
      s2.foreach { s =>
        val s3 = s.split("=")
        if (s3.length != 2) throw new IllegalArgumentException("invalid JDBC parameter")
        params += (s3(0) -> s3(1))
      }
    }
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // TODO: DataFrame -> RDD[T]
    sqlContext.read.format("jdbc").options(params).load().rdd.map(t => extract(t))
  }
}

object JdbcStorage {
  def apply[T <: SchemaClass: ClassTag](fraction: Int = -1): JdbcStorage[T] = new JdbcStorage[T](fraction)
}
