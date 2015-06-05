/*
 * Copyright (c) 2015 The Piglet team,
 *                    All Rights Reserved.
 *
 * This file is part of the Piglet package.
 *
 * PipeFabric is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License (GPL) as
 * published by the Free Software Foundation; either version 2 of
 * the License, or (at your option) any later version.
 *
 * This package is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; see the file LICENSE.
 * If not you can find the GPL at http://www.gnu.org/copyleft/gpl.html
 */
package dbis.pig.tools

import scala.io.Source
import scala.reflect.internal.util.BatchSourceFile
import scala.tools.nsc.io.AbstractFile
import scala.tools.nsc.reporters.ConsoleReporter
import scala.tools.nsc.{Global, Settings}

trait Probe

/**
 * ScalaCompiler is an object allowing to access the Scala compiler
 * for compiling source files.
 */

object ScalaCompiler {
  /**
   * Compiles the given Scala source file into a class file stored
   * in targetDir.
   *
   * @param targetDir the target directory for the compiled code
   * @param sourceFile the Scala file to be compiled
   */
  def compile (targetDir: String, sourceFile: String) : Boolean = {
    val target = AbstractFile.getDirectory(targetDir)
    val settings = new Settings
    /*
    settings.deprecation.value = true // enable detailed deprecation warnings
    settings.unchecked.value = true // enable detailed unchecked warnings
    settings.usejavacp.value = true
*/
    settings.outputDirs.setSingleOutput(target)
    settings.embeddedDefaults[Probe]
    val reporter = new ConsoleReporter(settings)

    val global = Global(settings, reporter)
    import global._

    val file = sourceFile
    val fileContent = Source.fromFile(file).mkString

    val run = new Run
    val sourceFiles = List(new BatchSourceFile(file, fileContent))
    run.compileSources(sourceFiles)

    !reporter.hasErrors
  }
}
