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
    case _ => throw new Exception(s"Backend $backend not available")
  }
}

object Dependencies {
  // Libraries
  val scalaCompiler = "org.scala-lang" % "scala-compiler" %  "2.11.7"
  val scalaTest = "org.scalatest" %% "scalatest" % "2.2.0"
  val scalaParserCombinators = "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.3"
  val scalaIoFile = "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.3-1"
  val jline = "jline" % "jline" % "2.12.1"
  val graphCore = "com.assembla.scala-incubator" %% "graph-core" % "1.9.2"
  val sparkCore = "org.apache.spark" %% "spark-core" % "1.4.0"
  val sparkSql = "org.apache.spark" %% "spark-sql" % "1.4.0"
  val flinkDist = "org.apache.flink" %% "flink-dist" % "0.9-SNAPSHOT"
  val scopt = "com.github.scopt" %% "scopt" % "3.3.0"
  val scalasti = "org.clapper" %% "scalasti" % "2.0.0"
  val jeromq = "org.zeromq" % "jeromq" % "0.3.4"
  val kiama = "com.googlecode.kiama" %% "kiama" % "1.8.0"
  val typesafe = "com.typesafe" % "config" % "1.3.0"
  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"
  val log4j = "log4j" % "log4j" % "1.2.17"

  // Projects
  val rootDeps = Seq(
    jline,
    scalaTest % "test,it" withSources(),
    scalaParserCombinators withSources(),
    scalaCompiler,
    scopt,
    scalaIoFile,
    scalasti,
    kiama,
    typesafe,
    scalaLogging,
    log4j
  )
}
