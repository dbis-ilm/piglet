import Dependencies._

name := "flinklib"

libraryDependencies ++= Seq(
    scalaCompiler,
    scalaTest % "test" withSources(),
    jeromq,
    flinkScala % "provided",
    flinkStreaming % "provided",
    typesafe,
    log4j,
    akkaLogging
)

resolvers += "Sonatype (releases)" at "https://oss.sonatype.org/content/repositories/releases/"

scalacOptions ++= Seq("-feature","-language:implicitConversions")

test in assembly := {}
logLevel in assembly := Level.Error
//
