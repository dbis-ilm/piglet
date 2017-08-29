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
package dbis.piglet.tools

import scala.io.Source
import scala.reflect.internal.util.BatchSourceFile
import scala.tools.nsc.io.AbstractFile
import scala.tools.nsc.reporters.ConsoleReporter
import scala.tools.nsc.{Global, Settings}
import java.nio.file.Path
import dbis.piglet.tools.logging.PigletLogging
import dbis.setm.SETM.timing

trait Probe

/**
 * ScalaCompiler is an object allowing to access the Scala compiler
 * for compiling source files.
 */

object ScalaCompiler extends PigletLogging {
  
  
  def compile (targetDir: Path, sourceFile: Path) : Boolean = compile(targetDir, Seq(sourceFile))
  
  /**
   * Compiles the given Scala source file into a class file stored
   * in targetDir.
   *
   * @param targetDir the target directory for the compiled code
   * @param sourceFiles the Scala files to be compiled
   */
  def compile (targetDir: Path, sourceFiles: Seq[Path]) : Boolean = timing("compile scala code") {
    
    logger.debug(s"""compiling source file '${sourceFiles.mkString(",")}' to target dir '$targetDir'""")
    
    val target = AbstractFile.getDirectory(targetDir.toFile)
    val settings = new Settings
    /*
    settings.deprecation.value = true // enable detailed deprecation warnings
    settings.unchecked.value = true // enable detailed unchecked warnings
    settings.usejavacp.value = true
*/
    settings.classpath.append(targetDir.toString)

    settings.outputDirs.setSingleOutput(target)
    settings.embeddedDefaults[Probe]
    settings.processArgumentString("-feature -language implicitConversions")

    // println("settings = " + settings)
    val reporter = new ConsoleReporter(settings)

    val global = Global(settings, reporter)
    import global._

    
    val sources = sourceFiles.map { f => 
        new BatchSourceFile(f.toString, Source.fromFile(f.toFile).mkString)
      }.toList
    
    val run = new Run
    run.compileSources(sources)

    !reporter.hasErrors
  }
}
