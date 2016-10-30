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

import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.ddahl.jvmr._

object RScriptOp {
  implicit def anyToDouble(a: Any) = a.toString.toDouble

  def rddToMatrix[T <: Any](rdd: RDD[List[T]]): Array[Array[Double]] = {
    val m = rdd.map(l => l.map(s => s.toDouble).toArray)
    m.collect
  }

  def process[T <: Any](sc: SparkContext, in: RDD[List[T]], script: String, resObj: String): RDD[List[Any]] = {
    // replace $_ by _input in the script -> that's the input parameter
    val rscript = script.replace("$_", "inp")
    // initialize the R interpreter
    val R = RInScala()
    // create an array from the RDD and assign it to input_
    R.inp = rddToMatrix(in)
    // run the R script
    R.eval(rscript)
    // fetch the result and convert it to a matrix
    val res: Array[Array[Double]] = R.toMatrix[Double](resObj)
    // construct a Seq of it and ...
    val rres = res.map(l => l.toList).toSeq
    // ... finally a RDD
    sc.parallelize(rres)
  }
}
