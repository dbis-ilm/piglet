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

package dbis.pig.backends.flink

import scala.Numeric.Implicits._
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import java.util.Random
import org.apache.flink.api.scala._
import dbis.pig.backends._
import org.apache.flink.api.java.functions._
import org.apache.flink.api.common.typeinfo.TypeInformation

class CustomSampler[T <: SchemaClass: ClassTag: TypeInformation](dataSet: DataSet[T]) {
  def sample(withReplacement: Boolean, fraction: Double, seed: Long = new Random().nextLong()) = {
    dataSet.mapPartition(new SampleWithFraction[T](withReplacement, fraction, seed))
  }

}

object Sampler {
  implicit def addSampler[T <: SchemaClass: ClassTag: TypeInformation](dataSet: DataSet[T]) = {
    new CustomSampler(dataSet)
  }
}

object PigFuncs {
  def average[T: Numeric](bag: Iterable[T]): Double = sum(bag).toDouble / count(bag).toDouble

  def count(bag: Iterable[Any]): Int = bag.size

  def sum[T: Numeric](bag: Iterable[T]): T = bag.sum

  def min[T: Ordering](bag: Iterable[T]): T = bag.min

  def max[T: Ordering](bag: Iterable[T]): T = bag.max

  def tokenize(s: String, delim: String = """[, "]""") = s.split(delim)

  def startswith(haystack: String, prefix: String) = haystack.startsWith(prefix)

  def strlen(s: String) = s.length()

  /**
   * Incremental versions of the aggregate functions - used for implementing ACCUMULATE.
   */
  def incrSUM(acc: Int, v: Int) = acc + v
  def incrSUM(acc: Double, v: Double) = acc + v
  def incrSUM(acc: Long, v: Long) = acc + v
  def incrCOUNT(acc: Int, v: Int) = acc + 1
  def incrCOUNT(acc: Long, v: Long) = acc + 1
  def incrCOUNT(acc: Double, v: Double) = acc + 1
  def incrMIN(acc: Int, v: Int) = math.min(acc, v)
  def incrMIN(acc: Long, v: Long) = math.min(acc, v)
  def incrMIN(acc: Double, v: Double) = math.min(acc, v)
  def incrMAX(acc: Int, v: Int) = math.max(acc, v)
  def incrMAX(acc: Long, v: Long) = math.max(acc, v)
  def incrMAX(acc: Double, v: Double) = math.max(acc, v)
}
