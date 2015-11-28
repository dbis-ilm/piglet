import Dependencies._

name := "common"

libraryDependencies ++= Seq(
	"ch.qos.logback" % "logback-classic" % "1.1.3",
	"org.slf4j" % "slf4j-api" % "1.7.13" % "provided",
	hadoop % "provided"
)
