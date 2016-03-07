import Dependencies._

name := "mapreduce"

libraryDependencies ++= Seq(
    scalaTest % "test" withSources(),
    pig % "provided",
    hadoop,
    typesafe
)

test in assembly := {}
