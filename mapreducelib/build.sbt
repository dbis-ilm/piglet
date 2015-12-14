import Dependencies._

name := "mapreduce"

libraryDependencies ++= Seq(
    scalaTest % "test" withSources(),
    pig % "provided",
    typesafe
)

test in assembly := {}
