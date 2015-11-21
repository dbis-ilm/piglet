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
import scala.io.Source
import scalax.file.Path
import dbis.test.CompileIt
import sys.process._

class FlinkCompileIt extends CompileIt {

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
    ("filter.pig", "filtered.out", "truth/filtered.data", true, "pig", "flink"),
    ("foreach1.pig", "distances.out", "truth/distances.data", true, "pig", "flink"),
    //("nforeach.pig", "nested.out", "truth/nested.data", true, "pig", "flink"), 
    //("groupforeach.pig", "groupedrdf.out", "truth/groupedrdf.data", true, "sparql", "spark"), //  the order in flink including the groupBy operator is not preserved ?? but the result is similar to spark
    //("nforeach2.pig", "rdf.out", "truth/rdf.data", true, "sparql", "flink"),
    ("grouping.pig", "grouping.out", "truth/grouping.data", false, "pig", "flink"),
    ("wordcount.pig", "marycounts.out", "truth/marycount.data", false, "pig", "spark"),
    ("bag.pig", "bag.out", "truth/bag.data", true, "pig", "spark"),
    ("construct.pig", "construct.out", "truth/construct.data", true, "pig", "spark"),
    ("union.pig", "united.out", "truth/united.data", true, "pig", "spark"),
    ("aggregate.pig", "aggregate.out", "truth/aggregate.data", false, "pig", "spark"),
    //("sampling.pig", "sampling.out", "truth/sampling.data", false, "pig", "spark"),
    ("embedded.pig", "embedded.out", "truth/embedded.data", true, "pig", "spark"),
    ("macro1.pig", "macro1.out", "truth/macro1.data", true, "pig", "spark")
  )

  "The Pig compiler" should "compile and execute the script" in {
    forAll(scripts) { (script: String, resultDir: String, truthFile: String, inOrder: Boolean, lang: String, backend: String) =>

      if (sys.env.get("FLINK_JAR").isEmpty) {
        println("FLINK_JAR variable not set - exiting.")
        System.exit(0)
      }

      if (availablePort(6123) == false ) { // or by checking the process of  job manager
        println("It seems that the job manager of" +
          "flink is not running. Please, run the local job manager" +
          "by executing ./start-local")
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

  def availablePort(port: Int): Boolean = {
    import java.net.ServerSocket
    import java.net.DatagramSocket
    import java.io.IOException
    var ss: ServerSocket = null
    var ds: DatagramSocket = null
    try {
      ss = new ServerSocket(port)
      ss.setReuseAddress(true)
      ds = new DatagramSocket(port)
      ds.setReuseAddress(true)
      return true
    } catch {
      case e: IOException =>
    } finally {
      if (ds != null) {
        ds.close()
      }

      if (ss != null) {
        try {
          ss.close()
        } catch {
          case e: IOException =>
        }
      }
    }

    return false
  }
}

