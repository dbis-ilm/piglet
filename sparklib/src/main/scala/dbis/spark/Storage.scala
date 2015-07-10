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

package dbis.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import java.io.FileInputStream
import java.io.ObjectOutputStream
import java.io.ObjectInputStream
import java.io.FileOutputStream


class PigStorage extends java.io.Serializable {
  def load(sc: SparkContext, path: String, delim: Char = '\t'): RDD[List[String]] = {
    sc.textFile(path).map(line => line.split(delim).toList)
  }

  def write(path: String, rdd: RDD[String]) {
    rdd.saveAsTextFile(path)
  }
  
  /*
  def load(sc: SparkContext, path: String, schema: Schema, delim: String = " "): RDD[List[Any]] = {
    val pattern = "[^,(){}]+".r
    val fields = schema.element.valueType.fields
    sc.textFile(path).map(line => {
      val strings = pattern.findAllIn(line).toList
      for (f <- fields) {
      }
    })
  }
  */
}

object PigStorage {
  def apply(): PigStorage = {
    new PigStorage
  }
}

class RDFFileStorage extends java.io.Serializable {
  val pattern = "([^\"]\\S*|\".+?\")\\s*".r

  def rdfize(line: String): Array[String] = {
    val fields = pattern.findAllIn(line).map(_.trim)
    fields.toArray.slice(0, 3)
  }

  def load(sc: SparkContext, path: String): RDD[Array[String]] = {
    sc.textFile(path).map(line => rdfize(line))
  }
}

object RDFFileStorage {
  def apply(): RDFFileStorage = {
    new RDFFileStorage
  }
}


class BinStorage extends java.io.Serializable {
  
  def load(sc: SparkContext, path: String): RDD[Any] = {
    
    val rdd = sc.objectFile[Any](path)
    
    return rdd
    
//    var ois: Option[ObjectInputStream] = None
//    
//    try {
//      ois = Some(new ObjectInputStream(new FileInputStream(path)))
//      val rdd = ois.get.readObject().asInstanceOf[RDD[Any]]
//      return rdd  
//      
//    } finally {
//      if(ois.isDefined)
//        ois.get.close()
//    }
  }
  
  def write(sc: SparkContext, path: String, rdd: RDD[Any]) = {
    
    rdd.saveAsObjectFile(path);
    
//    var oos: Option[ObjectOutputStream] = None
//    try {
//      oos = Some(new ObjectOutputStream(new FileOutputStream(path)))
//      oos.get.writeObject(rdd)
//    } finally {
//      if(oos.isDefined)
//        oos.get.close()
//    }
  }
  
}

object BinStorage {
  def apply(): BinStorage = new BinStorage
}