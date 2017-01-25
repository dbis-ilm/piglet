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
package dbis.test.flink

import org.scalatest.prop.TableDrivenPropertyChecks._
import dbis.test.CompileIt
import org.scalatest.FlatSpec

class FlinksCompileIt extends FlatSpec with CompileIt {
  val scripts = Table(
    ("script", "result", "truth", "inOrder", "backend"), // only the header of the table
    ("stream_load.pig",  "result1.out",   "truth/result1.data",   false, "flinks"),
    ("stream_load2.pig", "result2.out",   "truth/result2.data",   false, "flinks"),
    ("stream_foreach1.pig", "distances.out", "truth/distances.data", false, "flinks"),
    ("stream_filter.pig", "filtered.out",  "truth/filtered.data",  false, "flinks"),
//    ("construct.pig",     "result3.out", "truth/construct.data",   false, "streaming", "flinks"),
//    ("union.pig",         "united.out",    "truth/united.data",    false, "streaming", "flinks"),
    ("streaming/aggregate.pig",     "aggregate.out", "truth/aggregate2.data",false, "flinks"),
    ("splitInto.pig",     "splitX.out",    "truth/splitX.data",    false, "flinks"),
    ("windowJoin.pig",    "joinedW.out",   "truth/joined.data",    false,  "flinks"),
    ("windowCross.pig",   "crossedW.out",  "truth/crossed.data",   false, "flinks"),
    ("windowGrouping.pig","grouping.out",  "truth/grouping.data",  false, "flinks"),
    ("windowNforeach.pig","nested.out",    "truth/nested.data",    false,  "flinks"),
    ("windowFilter.pig",  "filtered.out",  "truth/filtered.data",  false,  "flinks"),
    ("windowCount.pig",   "marycounts.out","truth/marycount.data", false,  "flinks"),
    ("windowDistinct.pig","unique.out",    "truth/unique.data",    false, "flinks"),
    ("windowSort.pig",    "sorted.out",    "truth/sorted.data",    true, "flinks")
    //TODO: Sample, Split, Stream-Through, Hybrid-Operators
 )
  scripts.zipWithIndex foreach { case ((script, result, truth, inOrder, backend), i) =>
    checkMatch(script, result, truth, inOrder, backend, i, scripts.size) }

}
