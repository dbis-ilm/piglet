import sbt._
import Keys._
import sbtbuildinfo._
import sbtbuildinfo.BuildInfoKeys._

object PigBuild extends Build {

  /*
   * Common Settings **********************************************************
   */
  lazy val commonSettings = Seq(
    version := "0.3",
    scalaVersion := "2.11.8",
    organization := "dbis"
  )

  /*
   * Projects *****************************************************************
   */
  lazy val piglet = (project in file(".")).
    enablePlugins(BuildInfoPlugin).
    settings(
      buildInfoKeys := Seq[BuildInfoKey](
        name, version, scalaVersion, sbtVersion, buildInfoBuildNumber,
        "master" -> sys.props.getOrElse("master", default=s"local[${java.lang.Runtime.getRuntime.availableProcessors()}]")
      ),
      buildInfoOptions += BuildInfoOption.BuildTime,
      buildInfoPackage := "dbis.piglet"
    ).
    configs(IntegrationTest).
    settings(commonSettings: _*).
    settings(Defaults.itSettings: _*).
    dependsOn(common).
    dependsOn(sparklib % "test;it").
    dependsOn(flinklib % "test;it").
    dependsOn(mapreducelib % "test;it").
    dependsOn(ceplib % "test;it").
    dependsOn(setm).
//    dependsOn(ProjectRef(uri("https://github.com/sthagedorn/setm.git#master"), "setm")).
    aggregate(common, sparklib, flinklib, mapreducelib, ceplib) // remove this if you don't want to automatically build these projects when building piglet

  lazy val setm = (project in file("setm"))


  lazy val common = (project in file("common")).
    settings(commonSettings: _*).
    disablePlugins(sbtassembly.AssemblyPlugin)

  lazy val sparklib = (project in file("sparklib")).
    settings(commonSettings: _*).
    settings(unmanagedJars in Compile += file(s"./lib_unmanaged/jvmr_2.11-2.11.2.1.jar")).
    dependsOn(common).
    disablePlugins(sbtassembly.AssemblyPlugin)

  lazy val flinklib = (project in file("flinklib")).
    settings(commonSettings: _*).
    dependsOn(common)
//    .disablePlugins(sbtassembly.AssemblyPlugin)

  lazy val mapreducelib = (project in file("mapreducelib")).
    settings(commonSettings: _*).
    dependsOn(common).
    disablePlugins(sbtassembly.AssemblyPlugin)

  lazy val ceplib = (project in file("ceplib")).
      settings(commonSettings: _*).
      dependsOn(common)

  lazy val zeppelin = (project in file("zeppelin")).
    settings(commonSettings: _*).
    dependsOn(common).
    disablePlugins(sbtassembly.AssemblyPlugin).
    dependsOn(piglet)

  /*
   * define the backend for the compiler: currently we support spark and flink
   */
  val backend = sys.props.getOrElse("backend", default="spark")

  val itDeps = backend match {
    case "flink" | "flinks" => Seq(
       Dependencies.flinkScala % "test;it",
       Dependencies.flinkStreaming % "test;it"
    )
    case "spark" | "sparks" => Seq(
      Dependencies.sparkCore % "test;it",
      Dependencies.sparkSql % "test;it",
      Dependencies.jdbc % "test;it"
    )
    case "mapreduce" => Seq(Dependencies.pig % "test;it")
    case _ => println(s"Unsupported backend: $backend ! I don't know which dependencies to include!"); Seq.empty[ModuleID]
  }

  val itTests = backend match{
    case "flink" => Seq("dbis.test.flink.FlinkCompileIt")
    case "flinks" => Seq("dbis.test.flink.FlinksCompileIt")
    case "spark" => Seq("dbis.test.spark.SparkCompileIt")
    case "sparks" => Seq("dbis.test.spark.SparksCompileIt")
    case "mapreduce" => Seq.empty[String] // TODO
    case _ => println(s"Unsupported backend: $backend - Will execute no tests"); Seq.empty[String]
  }
}

/*
 * Dependencies
 */
object Dependencies {
  
  val sparkVersion = "2.0.2"
  val flinkVersion = "1.1.3"
  
  // Libraries
  val scalaLib = "org.scala-lang" % "scala-library" %  "2.11.8"
  val scalaCompiler = "org.scala-lang" % "scala-compiler" %  "2.11.8"
  val scalaTest = "org.scalatest" %% "scalatest" % "3.0.0"
  val scalaParserCombinators = "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.3"
  val scalaIoFile = "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.3-1"
  val jline = "jline" % "jline" % "2.13"
  
  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion
  val sparkREPL = "org.apache.spark" %% "spark-repl" % sparkVersion
  val sparkStreaming = "org.apache.spark" %% "spark-streaming" % sparkVersion
  
  val flinkScala = "org.apache.flink" %% "flink-scala" % flinkVersion
  val flinkStreaming = "org.apache.flink" %% "flink-streaming-scala" % flinkVersion
  
  val hadoop = "org.apache.hadoop" % "hadoop-client" % "2.7.1"
  val pig = "org.apache.pig" % "pig" % "0.15.0"

  val scopt = "com.github.scopt" %% "scopt" % "3.3.0"
  val scalasti = "org.clapper" %% "scalasti" % "2.0.0"
  val jeromq = "org.zeromq" % "jeromq" % "0.3.4"
  val kiama = "com.googlecode.kiama" %% "kiama" % "1.8.0"
  val typesafe = "com.typesafe" % "config" % "1.3.0"
  val commons = "org.apache.commons" % "commons-exec" % "1.3"
  val twitterUtil = "com.twitter" %% "util-eval" % "6.29.0"
  val jdbc = "com.h2database" % "h2" % "1.4.190"

  val breeze = "org.scalanlp" %% "breeze" % "0.11.2"
  val log4j = "log4j" % "log4j" % "1.2.17"

  val scalajhttp = "org.scalaj" %% "scalaj-http" % "2.3.0"
  val json4s = "org.json4s" %% "json4s-native" % "3.4.0"

  // Projects
  val rootDeps = Seq(
    scalaLib,
    jline,
    scalaTest % "test;it" withSources(),
    scalaParserCombinators withSources(),
    scalaCompiler,
    scopt,
    scalaIoFile,
    scalasti,
    kiama,
    typesafe,
//    scalikejdbc,
//    scalikejdbc_config,
    commons,
    hadoop % "provided",
    twitterUtil,
    jdbc,
    breeze,
    scalajhttp,
    json4s
  )
}
