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

package dbis.test

import org.scalatest.{ Matchers, FlatSpec }
import dbis.piglet.Piglet
import dbis.piglet.backends.BackendManager
import dbis.piglet.BuildInfo
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.prop.TableFor6
import scala.io.Source
import scalax.file.Path
import org.apache.commons.exec._
import org.apache.commons.exec.environment.EnvironmentUtils
import java.nio.charset.MalformedInputException

trait CompileIt extends Matchers {
  this: FlatSpec =>

    
  def checkMatch(scripts: TableFor6[String, String, String, Boolean, String, String]) {
    "The Pig compiler" should "compile and execute the script" in {
      
      var i = 1
      
      forEvery(scripts) { (script: String, resultDir: String, truthFile: String, inOrder: Boolean, lang: String, backend: String) =>
        // 1. make sure the output directory is empty
        cleanupResult(resultDir)
        cleanupResult(script.replace(".pig", ""))

        val resultPath = Path.fromString(new java.io.File(".").getCanonicalPath)./(resultDir)
        val resourcePath = getClass.getResource("").getPath + "../../../"
        // 2. compile and execute Pig script
        
        print(s"\rTesting: [${"#"*i}${" "*(scripts.size - i)}] ${i}/${scripts.size} : $script                                        ")
        i += 1
        
        runCompiler(script, resourcePath, resultPath, lang, backend) should be(true)

        val result = getResult(resultPath)

        result should not be (null)

        val truth = Source.fromFile(resourcePath + truthFile).getLines()

        // 4. compare both files
        if (inOrder)
          result should contain theSameElementsInOrderAs (truth.toTraversable)
        else
          result should contain theSameElementsAs (truth.toTraversable)
        // 5. delete the output directory*/
        cleanupResult(resultDir)
        cleanupResult(script.replace(".pig", ""))
      }
      
      println
    }
  }
  private def cleanupResult(dir: String): Unit = {
    import scalax.file.Path
    val path: Path = Path(dir)
    try {
      path.deleteRecursively(continueOnFailure = false)
    } catch {
      case e: java.io.IOException => // some file could not be deleted

    }

  }
  private def getResult(resultPath: Path): Seq[String] = {

    // 3. load the output file(s) and the truth file
    //val result = Source.fromFile(resultDir + "/part-00000").getLines()

    var result = Iterator[String]()
    val resultFile = new java.io.File(resultPath.path)

    if (resultFile.isFile)
      result ++= Source.fromFile(resultFile).getLines()
    else {
      /*
       * Read in all result files. There may be multiple files
       * if the RDD has more than one partition. We iterate over
       * all files (in order) and read their content       
       */
      for (file <- resultFile.listFiles().filter(_.getName.startsWith("part-")).sortBy { f => f.getName }) {
        result ++= Source.fromFile(file).getLines
      }
      
    }
    result.toSeq
  }

  private def runCompiler(script: String, resourceName: String, resultPath: Path, lang: String, backend: String): Boolean = {
//    val executeLine = "| execute: " + script + " |"
//    var frame = ""
//    for (c <- 1 to executeLine.length()) frame += "-"
//    print("\n" + frame + "\n" + executeLine + "\n" + frame)

    val params = new java.util.HashMap[String, Object]()
    params.put("backend", backend)
    params.put("languages", lang)
    params.put("master", BuildInfo.master)
    params.put("outdir", ".")
    params.put("log-level", "ERROR")
    params.put("params", s"inbase=$resourceName,outfile=${resultPath.path}")
    params.put("script", resourceName + script)
    val cmdLine = new CommandLine("script/piglet")

    cmdLine.addArgument("--backend")
    cmdLine.addArgument("${backend}")
    cmdLine.addArgument("--master")
    cmdLine.addArgument("${master}")
    cmdLine.addArgument("--log-level")
    cmdLine.addArgument("${log-level}")
//    cmdLine.addArgument("--languages")
//    cmdLine.addArgument("${languages}")
    cmdLine.addArgument("--outdir")
    cmdLine.addArgument("${outdir}")
    cmdLine.addArgument("--params")
    cmdLine.addArgument("${params}")
    cmdLine.addArgument("--keep")
    cmdLine.addArgument("${script}")
    cmdLine.addArgument("--quiet") // we don't want to see header output for tests

    cmdLine.setSubstitutionMap(params)

    val executor = new DefaultExecutor()
    executor.setExitValue(0)
    val watchdog = new ExecuteWatchdog(120000)
    executor.setWatchdog(watchdog)
    // println("EXECUTE: " + cmdLine)
    executor.execute(cmdLine) == 0
  }

}

