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

package dbis.piglet.backends.flink

import java.util.Random

import dbis.piglet.CommonPigFuncs
import dbis.piglet.backends._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions._
import org.apache.flink.api.scala._

import scala.reflect.ClassTag

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

object PigFuncs extends CommonPigFuncs {
}
