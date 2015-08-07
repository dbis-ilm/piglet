import sbt._
import Keys._

object PigBuild extends Build {

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
    dependsOn(common).
    dependsOn(sparklib % "test;it").
    dependsOn(flinklib % "test;it"). 
    aggregate(common, sparklib, flinklib) // remove this if you don't want to automatically build these projects when building piglet 

  lazy val common = (project in file("common")).
    settings(commonSettings: _*)

  lazy val sparklib = (project in file("sparklib")).
    settings(commonSettings: _*).
    dependsOn(common)

  lazy val flinklib = (project in file("flinklib")).
    settings(commonSettings: _*).
    dependsOn(common)

    
    

  /*
   * define the backend for the compiler: currently we support spark and flink
   */
//  val backend = sys.props.getOrElse("backend", default="spark")
 
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
  val hadoop = "org.apache.hadoop" % "hadoop-client" % "2.7.1"

  // Projects
  val rootDeps = Seq(
    jline,
    scalaTest % "test;it" withSources(),
    scalaParserCombinators withSources(),
    scalaCompiler,
    scopt,
    scalaIoFile,
    scalasti,
    kiama,
    typesafe,
    scalaLogging,
    log4j,
    hadoop % "provided",
    
    sparkCore % "test;it",
    sparkSql % "test;it",
    
    flinkDist % "test;it"  from "http://cloud01.prakinf.tu-ilmenau.de/flink-0.9.jar"
  )
}
