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

class FlinkCompileIt extends FlatSpec with CompileIt {

  val scripts = Table(
    ("script", "result", "truth", "inOrder", "language", "backend"), // only the header of the table

    //flink
    ("load.pig", "result1.out", "truth/result1.data", true, "pig", "flink"),
    ("load2.pig", "result2.out", "truth/result2.data", true, "pig", "flink"),
    ("load3.pig", "result3.out", "truth/result3.data", true, "pig", "flink"),
    ("selfjoin.pig", "joined.out", "truth/joined.data", false, "pig", "flink"),
    ("selfjoin_ambiguous_fieldnames.pig", "joined_ambiguous_fieldnames.out", "truth/joined_ambiguous_fieldnames.data",
      //Pigs OrderBy is not a stable sort
      false, "pig", "flink"),
    ("selfjoin_filtered.pig", "joined_filtered.out", "truth/joined_filtered.data", false, "pig", "flink"),
    ("sort.pig", "sorted.out", "truth/sorted.data", true, "pig", "flink"),
    ("sort_multiple_directions.pig", "sorted_multiple_directions.out",
      "truth/sorted_multiple_directions.data", true, "pig", "flink"),
    ("filter.pig", "filtered.out", "truth/filtered.data", true, "pig", "flink"),
    ("foreach1.pig", "distances.out", "truth/distances.data", true, "pig", "flink"),
    ("nforeach.pig", "nested.out", "truth/nested.data", false, "pig", "flink"), 
    //("groupforeach.pig", "groupedrdf.out", "truth/groupedrdf.data", true, "sparql", "spark"), //  the order in flink including the groupBy operator is not preserved ?? but the result is similar to spark
    //("nforeach2.pig", "rdf.out", "truth/rdf.data", true, "sparql", "flink"),  //  the same thing here
    ("grouping.pig", "grouping.out", "truth/grouping.data", false, "pig", "flink"),
    ("grouping2.pig", "grouping2.out", "truth/grouping2.data", false, "pig", "flink"),
    ("groupall.pig", "groupall.out", "truth/groupall.data", false, "pig", "flink"),
    ("wordcount.pig", "marycounts.out", "truth/marycount.data", false, "pig", "flink"),
    ("bag.pig", "bag.out", "truth/bag.data", true, "pig", "flink"),
    ("construct.pig", "construct.out", "truth/construct.data", true, "pig", "flink"),
    ("union.pig", "united.out", "truth/united.data", true, "pig", "flink"),
    ("aggregate.pig", "aggregate.out", "truth/aggregate.data", false, "pig", "flink"),
    ("sampling.pig", "sampling.out", "truth/sampling.data", false, "pig", "flink"),
    ("embedded.pig", "embedded.out", "truth/embedded.data", true, "pig", "flink"),
    ("macro1.pig", "macro1.out", "truth/macro1.data", true, "pig", "flink")
  )
  //Note: checking the flink jobmanager, whether it is running or not, is done in the piglet script
  //Note: checking the flink jar inclusion is done also in the piglet
  it should behave like checkMatch(scripts)
}

