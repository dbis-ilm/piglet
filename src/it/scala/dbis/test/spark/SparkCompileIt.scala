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

import org.scalatest.{ Matchers, FlatSpec }
import org.scalatest.prop.TableDrivenPropertyChecks._
import scala.io.Source
import scalax.file.Path
import dbis.test.CompileIt

class SparkCompileIt extends CompileIt {
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
    ("filter.pig", "filtered.out", "truth/filtered.data", true, "pig", "spark"),
    ("foreach1.pig", "distances.out", "truth/distances.data", true, "pig", "spark"),
    ("nforeach.pig", "nested.out", "truth/nested.data", true, "pig", "spark"),
    ("groupforeach.pig", "groupedrdf.out", "truth/groupedrdf.data", true, "sparql", "spark"),
    ("nforeach2.pig", "rdf.out", "truth/rdf.data", true, "sparql", "spark"),
    ("grouping.pig", "grouping.out", "truth/grouping.data", false, "pig", "spark"),
    ("groupall.pig", "groupall.out", "truth/groupall.data", false, "pig", "spark"),
    ("wordcount.pig", "marycounts.out", "truth/marycount.data", false, "pig", "spark"),
    ("bag.pig", "bag.out", "truth/bag.data", true, "pig", "spark"),
    ("construct.pig", "construct.out", "truth/construct.data", true, "pig", "spark"),
    ("union.pig", "united.out", "truth/united.data", true, "pig", "spark"),
    ("aggregate.pig", "aggregate.out", "truth/aggregate.data", false, "pig", "spark"),
    ("sampling.pig", "sampling.out", "truth/sampling.data", false, "pig", "spark"),
    ("accumulate.pig", "accumulate.out", "truth/accumulate.data", false, "pig", "spark"),
    ("embedded.pig", "embedded.out", "truth/embedded.data", true, "pig", "spark"),
    ("macro1.pig", "macro1.out", "truth/macro1.data", true, "pig", "spark"),
    /* Works, but requires a R installation
    ("rscript.pig", "cluster.out", "truth/cluster.data", true)
    */
    /* Not working yet
    ("json.pig", "json.out", "json.data", true), // not working yet
    */
    /* Works, but requires a H2 database and the corresponding JDBC driver */
    ("jdbc.pig", "jdbc.out", "truth/jdbc-data.data", true, "pig", "spark")
  // RDF integration tests don't work because the standard language feature is not sparqlpig
//    ("rdf_starjoin_plain.pig", "rdf_starjoin_plain.out", "truth/rdf_starjoin_plain.data", false),
//    ("rdf_pathjoin_plain.pig", "rdf_pathjoin_plain.out", "truth/rdf_pathjoin_plain.data", false)
  //  ("aggrwogrouping.pig", "aggrwogrouping.out", "truth/aggrwogrouping.data", true)

  )

  "The Pig compiler" should "compile and execute the script" in {
    forAll(scripts) { (script: String, resultDir: String, truthFile: String, inOrder: Boolean, lang: String, backend: String) =>

      if (sys.env.get("SPARK_JAR").isEmpty) {
        println("SPARK_JAR variable not set - exiting.")
        System.exit(0)
      }
      
      // 1. make sure the output directory is empty
      cleanupResult(resultDir)
      cleanupResult(script.replace(".pig", ""))

      val resultPath = Path.fromString(new java.io.File(".").getCanonicalPath)./(resultDir)
      val resourcePath = getClass.getResource("").getPath + "../../../"

        // 2. compile and execute Pig script
      runCompiler(script, resourcePath, resultPath, lang, backend) should be(true)
      
      val result = getResult(resultPath)

      result should not be (null)

      val truth = Source.fromFile(resourcePath + truthFile).getLines()

      // 4. compare both files
      if (inOrder)
        result should contain theSameElementsInOrderAs (truth.toTraversable)
      else
        result should contain theSameElementsAs (truth.toTraversable)
      // 5. delete the output directory
      cleanupResult(resultDir)
      cleanupResult(script.replace(".pig", ""))
    }
  }
}
