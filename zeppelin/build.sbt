import Dependencies._

name := "piglet-interpreter"

libraryDependencies ++= Seq(
/*
    scalaCompiler,
    scalaTest % "test" withSources(),
    sparkCore % "provided",
    sparkSql % "provided",
*/
    "org.apache.zeppelin" % "zeppelin-interpreter" % "0.5.0-incubating"
)

dependencyOverrides += "org.slf4j" % "slf4j-log4j12" % "1.7.5"

test in assembly := {}
