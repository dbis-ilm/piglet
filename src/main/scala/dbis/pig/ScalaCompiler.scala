package dbis.pig

/**
 * Created by kai on 10.04.15.
 */

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
  def compile (targetDir: String, sourceFile: String) : Unit = {
    val target = AbstractFile.getDirectory(targetDir)
    val settings = new Settings
    /*
    settings.deprecation.value = true // enable detailed deprecation warnings
    settings.unchecked.value = true // enable detailed unchecked warnings
    settings.usejavacp.value = true
*/
    settings.outputDirs.setSingleOutput(target)
    settings.embeddedDefaults[Probe]
    val global = Global(settings, new ConsoleReporter(settings))
    import global._

    val file = sourceFile
    val fileContent = Source.fromFile(file).mkString

    val run = new Run
    val sourceFiles = List(new BatchSourceFile(file, fileContent))
    run.compileSources(sourceFiles)
  }
}
