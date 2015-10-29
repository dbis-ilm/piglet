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
import org.apache.commons.exec._
import org.apache.commons.exec.environment.EnvironmentUtils

class SparkCompileIt extends FlatSpec with Matchers {
  val scripts = Table(
    ("script", "result", "truth", "inOrder", "language", "backend"), // only the header of the table
    ("load.pig", "result1.out", "truth/result1.data", true, "pig", "spark"),
    ("load2.pig", "result2.out", "truth/result2.data", true, "pig", "spark"),
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
    ("construct.pig", "result3.out", "truth/result3.data", true, "pig", "spark"),
    ("union.pig", "united.out", "truth/united.data", true, "pig", "spark"),
    ("aggregate.pig", "aggregate.out", "truth/aggregate.data", false, "pig", "spark"),
    ("sampling.pig", "sampling.out", "truth/sampling.data", false, "pig", "spark"),
    // ("embedded.pig", "embedded.out", "truth/embedded.data", true),
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

  def runCompiler(script: String, resourceName: String, resultPath: Path, lang: String, backend: String): Boolean = {
    println("execute: " + script)
    val params = new java.util.HashMap[String, Object]()
    params.put("backend", backend)
    params.put("language", lang)
    params.put("master", "local[2]")
    params.put("outdir", ".")
    params.put("params", s"inbase=$resourceName,outfile=${resultPath.path}")
    params.put("script", resourceName + script)
    val cmdLine = new CommandLine("script/piglet")

    cmdLine.addArgument("--backend")
    cmdLine.addArgument("${backend}")
    cmdLine.addArgument("--master")
    cmdLine.addArgument("${master}")
    cmdLine.addArgument("--language")
    cmdLine.addArgument("${language}")
    cmdLine.addArgument("--outdir")
    cmdLine.addArgument("${outdir}")
    cmdLine.addArgument("--params")
    cmdLine.addArgument("${params}")
    cmdLine.addArgument("${script}")

    cmdLine.setSubstitutionMap(params)

    val executor = new DefaultExecutor()
    executor.setExitValue(0)
    val watchdog = new ExecuteWatchdog(120000)
    executor.setWatchdog(watchdog)
    println("EXECUTE: " + cmdLine)
    executor.execute(cmdLine) == 0
  }

  "The Pig compiler" should "compile and execute the script" in {
    forAll(scripts) { (script: String, resultDir: String, truthFile: String, inOrder: Boolean, lang: String, backend: String) =>
      // 1. make sure the output directory is empty
      cleanupResult(resultDir)
      cleanupResult(script.replace(".pig",""))

      val resultPath = Path.fromString(new java.io.File(".").getCanonicalPath)./(resultDir)
      val resourcePath = getClass.getResource("").getPath + "../../../"

      // 2. compile and execute Pig script
      /*
      PigCompiler.main(Array("--backend", "spark",
        "--params", s"inbase=$resourcePath,outfile=${resultPath.path}",
        "--master", "local[2]",
        "--outdir", ".", resourcePath + script))
      println("execute: " + script)
      */
      runCompiler(script, resourcePath, resultPath, lang, backend) should be (true)
      
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
