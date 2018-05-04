import sbt.Keys._
import sbt._

name := "piglet"

libraryDependencies ++= Dependencies.rootDeps

libraryDependencies ++= itDeps

mainClass in (Compile, packageBin) := Some("dbis.piglet.PigletREPL")

mainClass in (Compile, run) := Some("dbis.piglet.PigletREPL")

assemblyJarName in assembly := "piglet.jar"

mainClass in assembly := Some("dbis.piglet.Piglet")
assemblyMergeStrategy in assembly := {
	case PathList(ps@_*) if ps.last == "libjansi.so" || ps.last == "libjansi.jnilib" || ps.last == "jansi.dll" => MergeStrategy.first // linux
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}


test in assembly := {}

logLevel in assembly := Level.Error

parallelExecution in ThisBuild := false

// needed for serialization/deserialization
fork in Test := true

// enable for debug support in eclipse
//javaOptions in (Test) += "-Xdebug"
//javaOptions in (Test) += "-Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8000"

fork in IntegrationTest := false

// scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature","-Ylog-classpath")
scalacOptions ++= Seq("-feature","-language:implicitConversions")

// run only those it tests, that are available for the selected backend
testOptions in IntegrationTest := Seq(
	Tests.Filter(s => itTests.contains(s)),
	Tests.Argument("-oDF")
)

coverageExcludedPackages := "<empty>;dbis.piglet.Piglet;dbis.piglet.plan.rewriting.internals.MaterializationSupport;dbis.piglet.plan.rewriting.internals.WindowSupport"

sourcesInBase := false
EclipseKeys.skipParents in ThisBuild := false  // to enable piglet (parent not only children) eclispe import

/*
  * Common Settings **********************************************************
  */
lazy val commonSettings = Seq(
  version := "0.3",
  scalaVersion := Dependencies.scalaVersion,
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



