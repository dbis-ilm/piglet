import sbt._
import Keys._
import sbtbuildinfo._
import sbtbuildinfo.BuildInfoPlugin
import sbtbuildinfo.BuildInfoKeys._

object PigBuild extends AutoPlugin with Build {

  /*
   * Common Settings **********************************************************
   */
  lazy val commonSettings = Seq(
    version := "1.0",
    scalaVersion := "2.11.7",
    organization := "dbis"
  )

  /*
   * Projects *****************************************************************
   */
  lazy val root = (project in file(".")).
    configs(IntegrationTest).
    settings(commonSettings: _*).
    settings(Defaults.itSettings: _*).
    settings(excludes(backendEnv): _*).
    aggregate(backendlib(backendEnv).map(a => a.project): _*).
    dependsOn(backendlib(backendEnv): _*)

  lazy val common = (project in file("common")).
    settings(commonSettings: _*)

  lazy val sparklib = (project in file("sparklib")).
    settings(commonSettings: _*).
    dependsOn(common)

  lazy val flinklib = (project in file("flinklib")).
    settings(commonSettings: _*).
    dependsOn(common)


  /*
   * Values *******************************************************************
   */

  /*
   * define the backend for the compiler: currently we support spark and flink
   */
  val backendEnv = sys.props.getOrElse("backend", default="spark")

  /*
   * Methods ******************************************************************
   */
  def backendlib(backend: String): List[ClasspathDep[ProjectReference]] = backend match {
    case "flink" => List(common, flinklib)
    case "spark" => List(common, sparklib)
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
    case _ => throw new Exception(s"Backend $backend not available")
  }
}

/*
 * Dependencies
 */
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
