import sbt.Keys._
import sbt._

/*
 * Dependencies
 */
object Dependencies {

  val sparkVersion = "2.1.1"
  val flinkVersion = "1.1.3"
  val scalaVersion = "2.11.8"

  // Libraries
  val scalaLib = "org.scala-lang" % "scala-library" % scalaVersion
  val scalaCompiler = "org.scala-lang" % "scala-compiler" %  scalaVersion

  val scalaTest = "org.scalatest" %% "scalatest" % "3.0.3"
  val scalaParserCombinators = "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.6"
  val scalaIoFile = "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.3-1"
  val jline = "jline" % "jline" % "2.14.5"

  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion
  val sparkREPL = "org.apache.spark" %% "spark-repl" % sparkVersion
  val sparkStreaming = "org.apache.spark" %% "spark-streaming" % sparkVersion

  val flinkScala = "org.apache.flink" %% "flink-scala" % flinkVersion
  val flinkStreaming = "org.apache.flink" %% "flink-streaming-scala" % flinkVersion

  val hadoop = "org.apache.hadoop" % "hadoop-client" % "2.7.3"
  val pig = "org.apache.pig" % "pig" % "0.15.0"

  val scopt = "com.github.scopt" %% "scopt" % "3.6.0"
  val scalasti = "org.clapper" %% "scalasti" % "3.0.1"
  val jeromq = "org.zeromq" % "jeromq" % "0.4.0"
  val kiama = "com.googlecode.kiama" %% "kiama" % "1.8.0"
  val typesafe = "com.typesafe" % "config" % "1.3.0"
  val commons = "org.apache.commons" % "commons-exec" % "1.3"
  val twitterUtil = "com.twitter" %% "util-eval" % "6.29.0"
  val jdbc = "com.h2database" % "h2" % "1.4.190"

  //val breeze = "org.scalanlp" %% "breeze" % "0.11.2"
  val log4j = "log4j" % "log4j" % "1.2.17"
  val slf4j = "org.slf4j" % "slf4j-api" % "1.7.25"


  val scalajhttp = "org.scalaj" %% "scalaj-http" % "2.3.0"
  val json4s = "org.json4s" %% "json4s-native" % "3.5.3"

//  val akkahttp = "com.typesafe.akka" %% "akka-http" % "10.0.7"
//  val akkaslf = "com.typesafe.akka" % "akka-slf4j_2.11" % "2.4.17"

  val akka = "com.typesafe.akka" % "akka-actor_2.11" % "2.5.2"
  val akkaLogging = "com.typesafe.akka" % "akka-slf4j_2.11" % "2.3.7" //"2.5.2"

  val graphcore = "org.scala-graph" %% "graph-core" % "1.11.5"
  val graphjson = "org.scala-graph" % "graph-json_2.11" % "1.11.0"

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
    //breeze,
    scalajhttp,
    json4s,
//    akkahttp,
    akka,
//    akkaLogging,
    graphcore,
    graphjson
  )
}