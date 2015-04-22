name := "PigParser"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.11" % "2.2.0" % "test" withSources(),
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.3" withSources(),
  "org.scala-lang" % "scala-compiler" % "2.11.6",
  "com.assembla.scala-incubator" %% "graph-core" % "1.9.1",
  "org.apache.spark" %% "spark-core" % "1.3.0" % "provided",
  "com.github.scopt" %% "scopt" % "3.3.0"
)

assemblyJarName in assembly := "PigCompiler.jar"

test in assembly := {}

mainClass in assembly := Some("dbis.pig.PigCompiler")
