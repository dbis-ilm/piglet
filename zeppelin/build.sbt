import Dependencies._

name := "piglet-interpreter"

unmanagedJars in Compile += file("zeppelin/lib_unmanaged/zeppelin-spark-0.6.0-incubating-SNAPSHOT.jar")

libraryDependencies ++= Seq(
    sparkCore % "provided",
    sparkSql % "provided",
    "org.apache.zeppelin" % "zeppelin-interpreter" % "0.5.0-incubating"
)

dependencyOverrides += "org.slf4j" % "slf4j-log4j12" % "1.7.5"

test in assembly := {}
