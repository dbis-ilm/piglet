import Dependencies._

name := "flinklib"

libraryDependencies ++= Seq(
    scalaCompiler,
    scalaTest % "test" withSources(),
    jeromq,
    flinkDist % "provided" from "http://cloud01.prakinf.tu-ilmenau.de/flink-0.9.jar",
    typesafe
)

resolvers += "Sonatype (releases)" at "https://oss.sonatype.org/content/repositories/releases/"

test in assembly := {}