import Dependencies._

name := "common"

libraryDependencies ++= Seq(
	"ch.qos.logback" % "logback-classic" % "1.2.3",
	"org.slf4j" % "slf4j-api" % "1.7.25" % "provided",
	hadoop % "provided",
    json4s
)
