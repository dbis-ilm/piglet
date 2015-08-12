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

package dbis.pig.tools

import java.io.{File, FileOutputStream, FileWriter, InputStream, OutputStream}
import java.util.jar.JarFile
import dbis.pig._
import dbis.pig.codegen.ScalaBackendCompile
import dbis.pig.plan.DataflowPlan
import com.typesafe.scalalogging.LazyLogging
import dbis.pig.backends.BackendManager
import dbis.pig.backends.BackendConf
import java.nio.file.Path
import java.nio.file.Files
import java.nio.file.Paths

object FileTools extends LazyLogging {
  def copyStream(istream : InputStream, ostream : OutputStream) : Unit = {
    var bytes =  new Array[Byte](1024)
    var len = -1
    while({ len = istream.read(bytes, 0, 1024); len != -1 })
      ostream.write(bytes, 0, len)
  }

  def extractJarToDir(jarName: String, outDir: Path): Unit = {
    
    logger.debug(s"extracting jar $jarName to $outDir" )
    
    val jar = new JarFile(jarName)
    val enu = jar.entries
    while (enu.hasMoreElements) {
      val entry = enu.nextElement
      val entryPath = entry.getName

      // we don't need the MANIFEST.MF file
      if (! entryPath.endsWith("MANIFEST.MF")) {

        // println("Extracting to " + outDir + "/" + entryPath)
        if (entry.isDirectory) {
          Files.createDirectories(outDir.resolve(entryPath))
        }
        else {
          val istream = jar.getInputStream(entry)
          val ostream = new FileOutputStream(outDir.resolve(entryPath).toFile())
          copyStream(istream, ostream)
          ostream.close
          istream.close
        }
      }
    }
  }
  
  def compileToJar(plan: DataflowPlan, scriptName: String, outDir: Path, compileOnly: Boolean = false, backendJar: Path, templateFile: String): Option[Path] = {
    // 4. compile it into Scala code for Spark
    val compiler = new ScalaBackendCompile(templateFile) 

    // 5. generate the Scala code
    val code = compiler.compile(scriptName, plan)

    logger.debug("successfully generated scala program")
    
    // 6. write it to a file

    val outputDir =  outDir.resolve(scriptName) //new File(s"$outDir${File.separator}${scriptName}")
    
    logger.debug(s"outputDir: $outputDir")
    
    if(!Files.exists(outputDir)) {
      Files.createDirectories(outputDir)
    }
    

    val outputDirectory = outputDir.resolve("out")  //s"${outputDir.getCanonicalPath}${File.separator}out"
    logger.debug(s"outputDirectory: $outputDirectory")
    
    // check whether output directory exists
    if(!Files.exists(outputDirectory)) {
      Files.createDirectory(outputDirectory)
    }
    
    val outputFile = outputDirectory.resolve(s"$scriptName.scala")
    logger.debug(s"outputFile: $outputFile")
    val writer = new FileWriter(outputFile.toFile())
    writer.append(code)
    writer.close()

    // 7. extract all additional jar files to output
    plan.additionalJars.foreach(jarFile => FileTools.extractJarToDir(jarFile, outputDirectory))
    
    // 8. copy the sparklib library to output
    val jobJar = backendJar.toAbsolutePath().toString()
    FileTools.extractJarToDir(jobJar, outputDirectory)
    
//    if (compileOnly) 
//      return false // sys.exit(0)

    // 9. compile the scala code
    if (!ScalaCompiler.compile(outputDirectory, outputFile))
      return None


    // 10. build a jar file
    val jarFile = Paths.get(outDir.toAbsolutePath().toString(), scriptName, s"$scriptName.jar") //s"$outDir${File.separator}${scriptName}${File.separator}${scriptName}.jar" //scriptName + ".jar"
    
    if(JarBuilder(outputDirectory, jarFile, verbose = false)) {
      logger.info(s"created job's jar file at $jarFile")
      return Some(jarFile)
    } else 
      return None
  }

  
  
//  private def getTemplateFile(backend: String): String = {
//    BuildSettings.backends.get(backend).get("templateFile")
//  }
//
//  def getRunner(backend: String): Run = { 
//    val className = BuildSettings.backends.get(backend).get("runClass")
//    Class.forName(className).newInstance().asInstanceOf[Run]
//  }
}
