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
package dbis.test.spark

import org.scalatest.prop.TableDrivenPropertyChecks._
import dbis.test.CompileIt
import org.scalatest.FlatSpec

class SparksCompileIt extends FlatSpec with CompileIt {
  val scripts = Table(
   ("script", "result", "truth", "inOrder", "backend"), // only the header of the table
    ("stream_load.pig",  "result1.out",   "truth/result1.data",   false, "sparks"),
    ("stream_load2.pig", "result2.out",   "truth/result2.data",   false, "sparks"),
    ("stream_foreach1.pig", "distances.out", "truth/distances.data", false, "sparks"),
    ("stream_filter.pig", "filtered.out",  "truth/filtered.data",  false,  "sparks"),
/*    ("construct.pig",     "result3.out", "truth/construct.data",   false, "streaming", "sparks"),
    ("union.pig",         "united.out",    "truth/united.data",    false, "streaming", "sparks"),
    ("aggregate.pig",     "aggregate.out", "truth/aggregate2.data",false, "streaming", "sparks"),*/
    ("splitInto.pig",     "splitX.out",    "truth/splitX.data",    false, "sparks"),
    ("windowJoin.pig",    "joinedW.out",   "truth/joined.data",    false, "sparks"),
//    ("windowCross.pig",   "crossedW.out",  "truth/crossed.data",   false, "streaming", "sparks"),
    ("windowGrouping.pig","grouping.out",  "truth/grouping.data",  false, "sparks"),
    ("windowNforeach.pig","nested.out",    "truth/nested.data",    false, "sparks"),
    ("windowFilter.pig",  "filtered.out",  "truth/filtered.data",  false, "sparks"),
    ("windowCount.pig",   "marycounts.out","truth/marycount.data", false, "sparks"),
    ("windowDistinct.pig","unique.out",    "truth/unique.data",    false, "sparks"),
    ("windowSort.pig",    "sorted.out",    "truth/sorted.data",    true, "sparks")
    //TODO: Sample, Split, Stream-Through, Hybrid-Operators
 )
 
 scripts.zipWithIndex foreach { case ((script, result, truth, inOrder, backend), i) =>
  checkMatch(script, result, truth, inOrder, backend, i, scripts.size) }
}
