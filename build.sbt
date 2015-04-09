name := "PigParser"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.11" % "2.2.0" % "test" withSources(),
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.3" withSources(),
  "com.assembla.scala-incubator" %% "graph-core" % "1.9.1"
)