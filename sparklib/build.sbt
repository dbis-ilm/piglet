import Dependencies._

name := "sparklib"

libraryDependencies ++= Seq(
    scalaCompiler,
    scalaTest % "test" withSources(),
    sparkCore % "provided",
    sparkSql % "provided",
    sparkStreaming % "provided",
    typesafe,
    scalikejdbc,
    scalikejdbc_config,
    h2Database
)

test in assembly := {}

sourcesInBase := false