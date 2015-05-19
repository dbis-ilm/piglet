name := "PigParser"

version := "1.0"

scalaVersion := "2.11.6"

// scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

/*
 * define the backend for the compiler: currently we support spark and flink
 */
val backend = "spark"

def backendDependencies(backend: String) = backend match {
  case "flink" => "org.apache.spark" %% "spark-core" % "1.3.0" % "provided"
  case "spark" => "org.apache.spark" %% "spark-core" % "1.3.0" % "provided"
}

libraryDependencies ++= Seq(
  "jline" % "jline" % "2.12.1",
  "org.scalatest" % "scalatest_2.11" % "2.2.0" % "test" withSources(),
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.3" withSources(),
  "org.scala-lang" % "scala-compiler" % "2.11.6",
  "com.assembla.scala-incubator" %% "graph-core" % "1.9.1",
  backendDependencies(backend),
  // "org.apache.spark" %% "spark-core" % "1.3.0" % "provided",
  "com.github.scopt" %% "scopt" % "3.3.0"
)

mainClass in (Compile, packageBin) := Some("dbis.pig.PigREPL")

assemblyJarName in assembly := "PigCompiler.jar"

test in assembly := {}

mainClass in assembly := Some("dbis.pig.PigCompiler")
