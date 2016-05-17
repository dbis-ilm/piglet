import Dependencies._

name := "ceplib"

libraryDependencies ++= Seq(
    scalaCompiler,
    scalaTest % "test" withSources(),
    sparkCore % "provided",
    sparkStreaming % "provided",
    flinkScala % "provided",
    flinkStreaming % "provided",
    typesafe,
    log4j
)

test in assembly := {}
