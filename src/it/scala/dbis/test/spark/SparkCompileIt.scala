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

import org.scalatest.FlatSpec
import org.scalatest.prop.TableDrivenPropertyChecks._
import dbis.test.CompileIt

class SparkCompileIt extends FlatSpec with CompileIt{
  val scripts = Table(
    ("script", "result", "truth", "inOrder", "language", "backend"), // only the header of the table
    //SPARK
    ("load.pig", "result1.out", "truth/result1.data", true, "pig", "spark"),
    ("load2.pig", "result2.out", "truth/result2.data", true, "pig", "spark"),
    ("load3.pig", "result3.out", "truth/result3.data", true, "pig", "spark"),
    ("selfjoin.pig", "joined.out", "truth/joined.data", true, "pig", "spark"),
    ("selfjoin_ambiguous_fieldnames.pig", "joined_ambiguous_fieldnames.out", "truth/joined_ambiguous_fieldnames.data",
      // Pigs OrderBy is not a stable sort
      false, "pig", "spark"),
    ("selfjoin_filtered.pig", "joined_filtered.out", "truth/joined_filtered.data", true, "pig", "spark"),
    ("sort.pig", "sorted.out", "truth/sorted.data", true, "pig", "spark"),
    ("sort_multiple_directions.pig", "sorted_multiple_directions.out",
      "truth/sorted_multiple_directions.data", true, "pig", "spark"),
    ("filter.pig", "filtered.out", "truth/filtered.data", true, "pig", "spark"),
    ("foreach1.pig", "distances.out", "truth/distances.data", true, "pig", "spark"),
    ("nforeach.pig", "nested.out", "truth/nested.data", true, "pig", "spark"),
    ("groupforeach.pig", "groupedrdf.out", "truth/groupedrdf.data", true, "sparql", "spark"),
    ("nforeach2.pig", "rdf.out", "truth/rdf.data", true, "sparql", "spark"),
    ("grouping.pig", "grouping.out", "truth/grouping.data", false, "pig", "spark"),
    ("grouping2.pig", "grouping2.out", "truth/grouping2.data", false, "pig", "spark"),
    ("groupall.pig", "groupall.out", "truth/groupall.data", false, "pig", "spark"),
    ("wordcount.pig", "marycounts.out", "truth/marycount.data", false, "pig", "spark"),
    ("bag.pig", "bag.out", "truth/bag.data", true, "pig", "spark"),
    ("construct.pig", "construct.out", "truth/construct.data", true, "pig", "spark"),
    ("union.pig", "united.out", "truth/united.data", true, "pig", "spark"),
    ("cross.pig", "crossed.out", "truth/cross2.csv", false, "pig", "spark"),
    ("aggregate.pig", "aggregate.out", "truth/aggregate.data", false, "pig", "spark"),
    ("sampling.pig", "sampling.out", "truth/sampling.data", false, "pig", "spark"),
    ("accumulate.pig", "accumulate.out", "truth/accumulate.data", false, "pig", "spark"),
    ("embedded.pig", "embedded.out", "truth/embedded.data", true, "pig", "spark"),
    ("macro1.pig", "macro1.out", "truth/macro1.data", true, "pig", "spark"),
    ("top.pig", "top.out", "truth/top.data", true, "pig", "spark"),
    ("top_schema.pig", "top_schema.out", "truth/top.data", true, "pig", "spark"),
    /* Works, but requires a R installation
    ("rscript.pig", "cluster.out", "truth/cluster.data", true)
    */
    /* Not working yet
    ("json.pig", "json.out", "json.data", true), // not working yet
    */
    /* Works, but requires a H2 database and the corresponding JDBC driver */
    ("jdbc.pig", "jdbc.out", "truth/jdbc-data.data", true, "pig", "spark"),
    ("simple_matrix.pig", "simple-matrix.out", "truth/simple-matrix-res.data", true, "pig", "spark"),

    ("rdf_starjoin_plain.pig", "rdf_starjoin_plain.out", "truth/rdf_starjoin_plain.data", false, "sparql", "spark"),
    ("rdf_pathjoin_plain.pig", "rdf_pathjoin_plain.out", "truth/rdf_pathjoin_plain.data", false, "sparql", "spark"),
   // ("aggrwogrouping.pig", "aggrwogrouping.out", "truth/aggrwogrouping.data", true, "plain", "spark")
    ("two_joins.pig", "twojoins.out", "truth/twojoins.data", false, "pig", "spark"),
    ("spatialfilter.pig", "spatialfilter.out", "truth/spatialfilter.data", false, "pig", "spark"),
    ("spatialfilterwithindex.pig", "spatialfilterwithinex.out", "truth/spatialfilter.data", false, "pig", "spark"),
    ("spatialjoin.pig", "spatialjoin.out", "truth/spatialjoin.data", false, "pig", "spark"),
    ("spatialjoinwithindex.pig", "spatialjoinwithindex.out", "truth/spatialjoin.data", false, "pig", "spark")

  )
  //Note: checking the spark jar inclusion is done also in the piglet script
  it should behave like checkMatch(scripts)
}
