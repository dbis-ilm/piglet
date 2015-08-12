import Dependencies._

name := "sparklib"

libraryDependencies ++= Seq(
    scalaCompiler,
    scalaTest % "test" withSources(),
    sparkCore % "provided",
    sparkSql % "provided",
    typesafe
)

test in assembly := {}