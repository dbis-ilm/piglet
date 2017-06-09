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
