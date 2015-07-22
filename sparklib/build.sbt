import Dependencies._

name := "sparklib"

libraryDependencies ++= Seq(
    scalaCompiler,
    scalaTest % "test" withSources(),
    sparkCore,
    sparkSql
)
