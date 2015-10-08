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

import dbis.pig.PigCompiler
import org.scalatest.{Matchers, FlatSpec}
import org.scalatest.prop.TableDrivenPropertyChecks._
import scala.io.Source
import scalax.file.Path

class SparkCompileIt extends FlatSpec with Matchers {
  val scripts = Table(
    ("script", "result", "truth", "inOrder"), // only the header of the table
    ("load.pig", "result1.out", "truth/result1.data", true),
    ("load2.pig", "result2.out", "truth/result2.data", true),
    ("selfjoin.pig", "joined.out", "truth/joined.data", true),
    ("selfjoin_ambiguous_fieldnames.pig", "joined_ambiguous_fieldnames.out", "truth/joined_ambiguous_fieldnames.data",
      // Pigs OrderBy is not a stable sort
      false),
    ("selfjoin_filtered.pig", "joined_filtered.out", "truth/joined_filtered.data", true),
    ("sort.pig", "sorted.out", "truth/sorted.data", true),
    ("filter.pig", "filtered.out", "truth/filtered.data", true),
    ("foreach1.pig", "distances.out", "truth/distances.data", true),
    ("nforeach.pig", "nested.out", "truth/nested.data", true),
    ("grouping.pig", "grouping.out", "truth/grouping.data", false),
    ("groupall.pig", "groupall.out", "truth/groupall.data", false),
    ("wordcount.pig", "marycounts.out", "truth/marycount.data", false),
    ("construct.pig", "result3.out", "truth/result3.data", true),
    ("union.pig", "united.out", "truth/united.data", true),
    ("aggregate.pig", "aggregate.out", "truth/aggregate.data", false),
    ("sampling.pig", "sampling.out", "truth/sampling.data", false),
    ("embedded.pig", "embedded.out", "truth/embedded.data", true)
    //("rscript.pig", "cluster.out", "truth/cluster.data", true), // requires an installation of R
    //("json.pig", "json.out", "json.data", true), // not working yet
    // ("jdbc.pig", "jdbc.out", "truth/jdbc-data.data", true) // requires a H2 database and the corresponding JDBC driver
  //  ("aggrwogrouping.pig", "aggrwogrouping.out", "truth/aggrwogrouping.data", true)
  )

  def cleanupResult(dir: String): Unit = {
    import scalax.file.Path

    val path: Path = Path(dir)
    try {
      path.deleteRecursively(continueOnFailure = false)
    }
    catch {
      case e: java.io.IOException => // some file could not be deleted
    }

  }

  "The Pig compiler" should "compile and execute the script" in {
    forAll(scripts) { (script: String, resultDir: String, truthFile: String, inOrder: Boolean) =>
      // 1. make sure the output directory is empty
      cleanupResult(resultDir)
      cleanupResult(script.replace(".pig",""))

      val resultPath = Path.fromString(new java.io.File(".").getCanonicalPath)./(resultDir)
      val resourcePath = getClass.getResource("").getPath + "../../../"

      // 2. compile and execute Pig script
      PigCompiler.main(Array("--backend", "spark",
        "--params", s"inbase=$resourcePath,outfile=${resultPath.path}",
        "--master", "local[2]",
        "--outdir", ".", resourcePath + script))
      println("execute: " + script)

      // 3. load the output file and the truth file
      val result = Source.fromFile(resultDir + "/part-00000").getLines()
      val truth = Source.fromFile(resourcePath + truthFile).getLines()
      // 4. compare both files
      if (inOrder)
        result.toSeq should contain theSameElementsInOrderAs (truth.toTraversable)
      else
        result.toSeq should contain theSameElementsAs (truth.toTraversable)

      // 5. delete the output directory
      cleanupResult(resultDir)
      cleanupResult(script.replace(".pig",""))
    }
  }
}
