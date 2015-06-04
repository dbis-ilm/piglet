package dbis.pig

import java.io.InputStream
import java.io.OutputStream
import java.io.File
import java.io.FileWriter
import java.io.FileOutputStream

import java.util.jar.JarFile

object FileTools {
  def copyStream(istream : InputStream, ostream : OutputStream) : Unit = {
    var bytes =  new Array[Byte](1024)
    var len = -1
    while({ len = istream.read(bytes, 0, 1024); len != -1 })
      ostream.write(bytes, 0, len)
  }

  def extractJarToDir(jarName: String, outDir: String): Unit = {
    val jar = new JarFile(jarName)
    val enu = jar.entries
    while (enu.hasMoreElements) {
      val entry = enu.nextElement
      val entryPath = entry.getName

      // we don't need the MANIFEST.MF file
      if (! entryPath.endsWith("MANIFEST.MF")) {

        // println("Extracting to " + outDir + "/" + entryPath)
        if (entry.isDirectory) {
          new File(outDir, entryPath).mkdirs
        }
        else {
          val istream = jar.getInputStream(entry)
          val ostream = new FileOutputStream(new File(outDir, entryPath))
          copyStream(istream, ostream)
          ostream.close
          istream.close
        }
      }
    }
  }
  
  def compileToJar(plan: DataflowPlan, scriptName: String, outDir: String, compileOnly: Boolean = false, backend: String = "spark"): Boolean = {
    // 4. compile it into Scala code for Spark
    val compiler = new ScalaBackendCompile(getTemplateFile(backend)) 

    val code = compiler.compile(scriptName, plan)

    // 5. write it to a file

    val outputDir = new File(s"$outDir${File.separator}${scriptName}")
    if(!outputDir.exists()) {
      outputDir.mkdirs()
    }

    val outputFile = s"$outputDir${File.separator}${scriptName}.scala" //scriptName + ".scala"
    val writer = new FileWriter(outputFile)
    writer.append(code)
    writer.close()

    if (compileOnly) false // sys.exit(0)

      // 6. compile the Scala code
    val outputDirectory = outputDir.getCanonicalPath + s"${File.separator}out"

    // check whether output directory exists
    val dirFile = new File(outputDirectory)
    // if not then create it
    if (!dirFile.exists)
      dirFile.mkdir()

    if (!ScalaCompiler.compile(outputDirectory, outputFile))
      false

    // 7. extract all additional jar files to output
    plan.additionalJars.foreach(jarFile => FileTools.extractJarToDir(jarFile, outputDirectory))

    // 8. copy the sparklib library to output
    backend match {
      case "flink" => FileTools.extractJarToDir("flinklib/target/scala-2.11/flinklib_2.11-1.0.jar", outputDirectory)
      case "spark" => FileTools.extractJarToDir("sparklib/target/scala-2.11/sparklib_2.11-1.0.jar", outputDirectory)
    }

    // 9. build a jar file
    val jarFile = s"$outDir${File.separator}${scriptName}${File.separator}${scriptName}.jar" //scriptName + ".jar"
    JarBuilder.apply(outputDirectory, jarFile, verbose = false)
    true
  }

  private def getTemplateFile(backend: String): String = {
    BuildSettings.backends.get(backend).get("templateFile")
  }

  def getRunner(backend: String): Run = { 
    val className = BuildSettings.backends.get(backend).get("runClass")
    Class.forName(className).newInstance().asInstanceOf[Run]
  }
}
