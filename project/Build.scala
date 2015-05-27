import sbt._
import Keys._

object PigBuild extends Build {

   val flinkSettings = Map(
    "name"  -> "flink",
    "class" -> "dbis.pig.FlinkCompile"
  )

  val sparkSettings = Map(
    "name"  -> "spark",
    "class" -> "dbis.pig.SparkCompile"
  )

  val flinkBackend =      Map("flink" -> flinkSettings, "default" -> flinkSettings)
  val sparkBackend =      Map("spark" -> sparkSettings, "default" -> sparkSettings)
  val flinksparkBackend = Map("flink" -> flinkSettings, "spark" -> sparkSettings, "default" -> sparkSettings)

  // For Testing:
  //lazy val print = taskKey[Unit]("Print out")
  //print := println(backends.value.get("default").get("name"))

  def backendDependencies(backend: String) = backend match {
    case "flink" => Seq (
      Dependencies.flinkDist % "provided" from "http://cloud01.prakinf.tu-ilmenau.de/flink-0.9.jar"
    )
    case "spark" => Seq (
      Dependencies.sparkCore
    )
    case "sparkflink" => Seq(
      Dependencies.flinkDist % "provided" from "http://cloud01.prakinf.tu-ilmenau.de/flink-0.9.jar",
      Dependencies.sparkCore
    )
    case _ => throw new Exception(s"Backend $backend not available")
  }

  def excludes(backend: String) = backend match{
    case "flink" => {
      excludeFilter in unmanagedSources :=
      HiddenFileFilter            ||
      "*SparkCompileSpec.scala"   ||
      "*CompileSpec.scala"
    } 
    case "spark" =>{
      excludeFilter in unmanagedSources :=
      HiddenFileFilter            ||
      "*FlinkCompileIt.scala"     ||
      "*FlinkCompileSpec.scala"   ||
      "*flink-template.stg"
    }
    case "sparkflink" => excludeFilter in unmanagedSources := HiddenFileFilter
    case _ => throw new Exception(s"Backend $backend not available")
  }
}

object Dependencies {
  // Versions
  lazy val scalaVersion =       "2.11.6"
  lazy val scalaTestVersion =   "2.2.0"
  lazy val scalaPCVersion =     "1.0.3"
  lazy val scalaIoFileVersion = "0.4.3-1"
  lazy val jlineVersion =       "2.12.1"
  lazy val graphVersion =       "1.9.2"
  lazy val sparkVersion =       "1.3.0"
  lazy val flinkVersion =       "0.9-SNAPSHOT"
  lazy val scoptVersion =       "3.3.0"
  lazy val scalastiVersion =    "2.0.0"

  // Libraries
  val scalaCompiler = "org.scala-lang" % "scala-compiler" % scalaVersion
  val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion
  val scalaParserCombinators = "org.scala-lang.modules" %% "scala-parser-combinators" % scalaPCVersion
  val scalaIoFile = "com.github.scala-incubator.io" %% "scala-io-file" % scalaIoFileVersion
  val jline = "jline" % "jline" % jlineVersion
  val graphCore = "com.assembla.scala-incubator" %% "graph-core" % graphVersion
  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion
  val flinkDist = "org.apache.flink" %% "flink-dist" % flinkVersion
  val scopt = "com.github.scopt" %% "scopt" % scoptVersion
  val scalasti = "org.clapper" %% "scalasti" % scalastiVersion

  // Projects
  val rootDeps = Seq(
    jline, 
    scalaTest % "test,it" withSources(),
    scalaParserCombinators withSources(),
    scalaCompiler,
    graphCore,
    sparkCore % "provided", //Delete Later
    scopt,
    scalaIoFile,
    scalasti
  )
  val sparkDeps = Seq(
    scalaTest % "test" withSources(),
    scalaCompiler,
    sparkCore
  )
  val flinkDeps = Seq(
    scalaTest % "test" withSources(),
    scalaCompiler,
    flinkDist % "provided" from "http://cloud01.prakinf.tu-ilmenau.de/flink-0.9.jar"
  )
}
