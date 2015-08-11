import Dependencies._

name := "flinklib"

libraryDependencies ++= Seq(
    scalaCompiler,
    scalaTest % "test" withSources(),
    jeromq,
    flinkDist % "provided" from flinkAddress,
    typesafe,
    scalaLogging
)

resolvers += "Sonatype (releases)" at "https://oss.sonatype.org/content/repositories/releases/"

test in assembly := {}
