import sbt._
import Keys._

object PigBuild extends Build {

  val possibleBackends = List("flink","spark","sparkflink")

  val flinkSettings = Map(
    "name"  -> "flink",
    "runClass" -> "dbis.pig.tools.FlinkRun",
//    "compilerClass" -> "dbis.pig.FlinkCompile",
    "templateFile" -> "flink-template.stg"//"src/main/resources/flink-template.stg"
  )

  val sparkSettings = Map(
    "name"  -> "spark",
    "runClass" -> "dbis.pig.tools.SparkRun",
//    "compilerClass" -> "dbis.pig.SparkCompile",
    "templateFile" -> "spark-template.stg"//"src/main/resources/spark-template.stg"
  )

  val flinkBackend =      Map("flink" -> flinkSettings, "default" -> flinkSettings)
  val sparkBackend =      Map("spark" -> sparkSettings, "default" -> sparkSettings)
  val flinksparkBackend = Map("flink" -> flinkSettings, "spark" -> sparkSettings, "default" -> sparkSettings)

  def backendDependencies(backend: String): Seq[sbt.ModuleID] = backend match {
    case "flink" => Seq (
      Dependencies.flinkDist % "provided" from "http://cloud01.prakinf.tu-ilmenau.de/flink-0.9.jar"
    )
    case "spark" => Seq (
      Dependencies.sparkCore % "provided"
    )
    case "sparkflink" => Seq(
      Dependencies.flinkDist % "provided" from "http://cloud01.prakinf.tu-ilmenau.de/flink-0.9.jar",
      Dependencies.sparkCore % "provided"
    )
    case _ => throw new Exception(s"Backend $backend not available")
  }

  def excludes(backend: String): Seq[sbt.Def.SettingsDefinition] = backend match{
    case "flink" => { Seq(
      excludeFilter in unmanagedSources :=
      HiddenFileFilter            ||
      "*SparkRun.scala"           ||
      "*SparkCompile.scala"       ||
      "*SparkCompileIt.scala"     ||
      "*SparkCompileSpec.scala",
      excludeFilter in unmanagedResources := 
      HiddenFileFilter || 
      "spark-template.stg"
    )} 
    case "spark" =>{ Seq(
      excludeFilter in unmanagedSources :=
      HiddenFileFilter            ||
      "*FlinkRun.scala"           ||
      "*FlinkCompile.scala"       ||
      "*FlinkCompileIt.scala"     ||
      "*FlinkCompileSpec.scala",
      excludeFilter in unmanagedResources := 
      HiddenFileFilter || 
      "flink-template.stg"
    )}
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
    scopt,
    scalaIoFile,
    scalasti
  )
  val sparkDeps = Seq(
    scalaTest % "test" withSources(),
    scalaCompiler,
    sparkCore % "provided"
  )
  val flinkDeps = Seq(
    scalaTest % "test" withSources(),
    scalaCompiler,
    flinkDist % "provided" from "http://cloud01.prakinf.tu-ilmenau.de/flink-0.9.jar"
  )
}
