import Dependencies._

name := "sparklib"

libraryDependencies ++= Seq(
    scalaCompiler,
    scalaTest % "test" withSources(),
    sparkCore % "provided",
    //sparkREPL % "provided", // doesn't work yet due to some incompatibilities with jetty
    sparkSql % "provided",
    sparkStreaming % "provided",
    typesafe,
    //scalikejdbc,
    //scalikejdbc_config,
    jdbc,
    scalajhttp
)

test in assembly := {}

scalacOptions ++= Seq("-feature","-language:implicitConversions")

sourcesInBase := false
