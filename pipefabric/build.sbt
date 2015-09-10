import Dependencies._

name := "pipefabric"

libraryDependencies ++= Seq(
    scalaCompiler,
    scalaTest % "test" withSources(),
    scalaLogging,
    typesafe
)
                  
test in assembly := {}
