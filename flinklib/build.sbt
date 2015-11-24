import Dependencies._

name := "flinklib"

libraryDependencies ++= Seq(
    scalaCompiler,
    scalaTest % "test" withSources(),
    jeromq,
    flinkCore % "provided",
    flinkStreaming % "provided",
    typesafe
)

resolvers += "Sonatype (releases)" at "https://oss.sonatype.org/content/repositories/releases/"

test in assembly := {}
