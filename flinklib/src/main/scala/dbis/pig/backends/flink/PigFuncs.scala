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
  def sample(withReplacement: Boolean,fraction: Double, seed: Long = new Random().nextLong() )  = {
    dataSet.mapPartition(new SampleWithFraction[T](withReplacement, fraction, seed))
  }

}

object Sampler {
  implicit def addSampler[T <: SchemaClass: ClassTag: TypeInformation](dataSet: DataSet[T]) = {
    new CustomSampler(dataSet)
  }
}

object PigFuncs {
  def average[T: Numeric](bag: Iterable[T]) : Double = sum(bag).toDouble / count(bag).toDouble

  def count(bag: Iterable[Any]): Long = bag.size

  def sum[T: Numeric](bag: Iterable[T]): T = bag.sum

  def min[T: Ordering](bag: Iterable[T]): T = bag.min

  def max[T: Ordering](bag: Iterable[T]): T = bag.max

  def tokenize(s: String, delim: String = """[, "]""") = s.split(delim)

  def startswith(haystack: String, prefix: String) = haystack.startsWith(prefix)
  
  def strlen(s: String) = s.length()
  
  def streamCount(field: Int) = (in:List[Any], state: Option[Long]) => {
    val currentState = state.getOrElse(0L)
    val count = currentState+1
    (in.updated(field,count), Some(count)) 
  }

  def streamCount(field: Int,field2: Int) = (in:List[Any], state: Option[Long]) => {
    val currentState = state.getOrElse(0L)
    val count = currentState+1
    (in.updated(field, 
      in(field).asInstanceOf[List[Any]].updated(0, 
        in(field).asInstanceOf[List[Any]](0).asInstanceOf[List[Any]].:+(count))), Some(count)) 
  }

  def streamSum(field: Int) = (in: List[Any], state: Option[Long]) => {
    val currentState:Long = state.getOrElse(0L)
    val updatedState = currentState + in(field).asInstanceOf[Int]
    (in.updated(field, updatedState), Some(updatedState))
  }

  def streamSum(field: Int,field2: Int) = (in:List[Any], state: Option[Long]) => {
    val currentState = state.getOrElse(0L)
    val sum = currentState + in(field).asInstanceOf[List[Any]](0).asInstanceOf[List[Any]](field2).asInstanceOf[String].toInt
    (in.updated(field, 
      in(field).asInstanceOf[List[Any]].updated(0, 
        in(field).asInstanceOf[List[Any]](0).asInstanceOf[List[Any]].:+(sum))), Some(sum)) 
  }

  def streamAvg(field: Int) = (in: List[Any], state: Option[(Double, Long)]) => {
    val currentState:Tuple2[Double, Long] = state.getOrElse(Tuple2(0.0, 0))
    val updatedState = (currentState._1 + in(field).asInstanceOf[Int], currentState._2 + 1)
    val avg = (updatedState._1 / updatedState._2)
    (in.updated(field, avg), Some(updatedState))
  }

  /*
   * Global Streaming Function
   */

  def streamFunc(fields: List[(String,List[Int])]) = (in: List[Any], state:Option[List[Any]]) => {
    if (fields.head._2.size>1) streamFuncG(fields,in,state) else streamFuncS(fields,in,state)
  }

  // For grouped Tuples
  def streamFuncG(fields: List[(String, List[Int])], in: List[Any], state:Option[List[Any]]) = {

    // Old or initial state
    val currentState = state.getOrElse(fields.map{f => f._1 match {
        case "AVG" => (0.0D, 0L)
        case "SUM" => (0.0D)
        case "COUNT" => (0L)
        case "MAX" => (Double.MinValue)
        case "MIN" => (Double.MaxValue)
    }})

    // Update state
    val updatedState = currentState.zip(fields).map{a => a._2._1 match {
      case "AVG" => {
        val avgState = a._1.asInstanceOf[(Double,Long)]
        (avgState._1 + in(1).asInstanceOf[List[Any]](0).asInstanceOf[List[Any]](a._2._2(1)).asInstanceOf[Int].toDouble,
         avgState._2 + 1)
      }
      case "SUM" => (a._1.asInstanceOf[Double] +  in(1).asInstanceOf[List[Any]](0).asInstanceOf[List[Any]](a._2._2(1)).asInstanceOf[Int].toDouble)
      case "COUNT" => (a._1.asInstanceOf[Long] + 1)
      case "MAX" => Math.max(a._1.asInstanceOf[Double], in(1).asInstanceOf[List[Any]](0).asInstanceOf[List[Any]](a._2._2(1)).asInstanceOf[Int].toDouble)
      case "MIN" => Math.min(a._1.asInstanceOf[Double], in(1).asInstanceOf[List[Any]](0).asInstanceOf[List[Any]](a._2._2(1)).asInstanceOf[Int].toDouble)
    }}

    // Update output Value
    val updatedValue = updatedState.zip(fields.map(_._1)).map{ v => v._2 match {
      case "AVG" => {
        val avgUpdatedState = v._1.asInstanceOf[(Double,Long)]
        (avgUpdatedState._1 / avgUpdatedState._2)
      }
      case _ => v._1
    }}

    // Add aggregates to output List
    val output =  in.updated(1, 
      in(1).asInstanceOf[List[Any]].updated(0, 
        in(1).asInstanceOf[List[Any]](0).asInstanceOf[List[Any]].++(updatedValue))) 

    (output, Some(updatedState))
  }

  // For flattened Tuples
  def streamFuncS(fields: List[(String, List[Int])], in: List[Any], state:Option[List[Any]]) = {

    // Old or initial state
    val currentState = state.getOrElse(fields.map{f => f._1 match {
        case "AVG" => (0.0D, 0L)
        case "SUM" => (0.0D)
        case "COUNT" => (0L)
        case "MAX" => (Double.MinValue)
        case "MIN" => (Double.MaxValue)
      }})

    // Update state
    val updatedState = currentState.zip(fields).map{a => a._2._1 match {
      case "AVG" => {
        val avgState = a._1.asInstanceOf[(Double,Long)]
        (avgState._1 + in(a._2._2(0)).asInstanceOf[Int].toDouble,
         avgState._2 + 1)
      }
      case "SUM" => (a._1.asInstanceOf[Double] +  in(a._2._2(0)).asInstanceOf[Int].toDouble)
      case "COUNT" => (a._1.asInstanceOf[Long] + 1)
      case "MAX" => Math.max(a._1.asInstanceOf[Double], in(a._2._2(0)).asInstanceOf[Int].toDouble)
      case "MIN" => Math.min(a._1.asInstanceOf[Double], in(a._2._2(0)).asInstanceOf[Int].toDouble)
    }}

    // Update output Value
    val updatedValue = updatedState.zip(fields.map(_._1)).map{ v => v._2 match {
      case "AVG" => {
        val avgUpdatedState = v._1.asInstanceOf[(Double,Long)]
        (avgUpdatedState._1 / avgUpdatedState._2)
      }
      case _ => v._1
    }}

    // Add aggregates to output List
    val output =  in.++(updatedValue) 

    (output, Some(updatedState))
  }

}
