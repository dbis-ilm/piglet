package dbis.pig

import java.io._
import java.util.jar._

import org.apache.spark.deploy.SparkSubmit
import scopt.OptionParser

import scala.io.Source
import sys.process._

/**
 * Created by kai on 31.03.15.
 */
object PigCompiler extends PigParser {
  def parseScriptFromSource(source: Source): List[PigOperator] = {
    parseScript(source.getLines().mkString)
  }

  case class CompilerConfig(master: String = "local", input: String = "", target: String = "flink", compile: Boolean = false, outDir: String = ".")

  def main(args: Array[String]): Unit = {
    var master: String = "local"
    var target: String = "flink"
    var inputFile: String = null
    var compileOnly: Boolean = false
    var outDir: String = null

    val parser = new OptionParser[CompilerConfig]("PigCompiler") {
      head("PigCompiler", "0.1")
      opt[String]('t', "target") optional() action { (x, c) => c.copy(target = x) } text ("spark, flink")
      opt[String]('m', "master") optional() action { (x, c) => c.copy(master = x) } text ("spark://host:port, mesos://host:port, yarn, or local.")
      opt[Unit]('c', "compile") action { (_, c) => c.copy(compile = true) } text ("compile only (don't execute the script)")
      opt[String]('o',"outdir") optional() action { (x, c) => c.copy(outDir = x)} text ("output directory for generated code")
      help("help") text ("prints this usage text")
      version("version") text ("prints this version info")
      arg[String]("<file>") required() action { (x, c) => c.copy(input = x) } text ("Pig file")
    }
    // parser.parse returns Option[C]
    parser.parse(args, CompilerConfig()) match {
      case Some(config) => {
        // do stuff
        master = config.master
        target = config.target
        inputFile = config.input
        compileOnly = config.compile
        outDir = config.outDir
      }
      case None =>
        // arguments are bad, error message will have been displayed
        return
    }

    // 1. we read the Pig file
    val source = Source.fromFile(inputFile)

    val fileName = new File(inputFile).getName

    // 2. then we parse it and construct a dataflow plan
    val plan = new DataflowPlan(parseScriptFromSource(source))
    if (!plan.checkSchemaConformance) {
      println("ERROR: schema conformance")
      return
    }

    val scriptName = fileName.replace(".pig", "")

    // 3. now, we should apply optimizations

    if (compileToJar(plan, scriptName, outDir, target, compileOnly)) {
      val jarFile = s"$outDir${File.separator}${scriptName}${File.separator}${scriptName}.jar"
      
      // 4. and finally call SparkSubmit
      if (target == "spark")
        SparkSubmit.main(Array("--master", master, "--class", scriptName, jarFile))
      else
        s"java -Dscala.usejavacp=true -cp ${jarFile} ${scriptName}" !
    }
  }

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

  def compileToJar(plan: DataflowPlan, scriptName: String, outDir: String, target: String, compileOnly: Boolean = false): Boolean = {
    // 4. compile it into Scala code for Spark or Flink
    val compile = if(target == "flink") (new FlinkCompile) 
                  else (new SparkCompile)
//    val compile = new SparkCompile
    val code = compile.compile(scriptName, plan)

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
    plan.additionalJars.foreach(jarFile => extractJarToDir(jarFile, outputDirectory))

    // 8. copy the sparklib library to output
    if (target == "flink")  
      extractJarToDir("flinklib/target/scala-2.11/flinklib_2.11-1.0.jar", outputDirectory)
    else
      extractJarToDir("sparklib/target/scala-2.11/sparklib_2.11-1.0.jar", outputDirectory)

    // 9. build a jar file
    val jarFile = s"$outDir${File.separator}${scriptName}${File.separator}${scriptName}.jar" //scriptName + ".jar"
    JarBuilder.apply(outputDirectory, jarFile, verbose = false)
    true
  }
}
