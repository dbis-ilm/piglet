name := "flinklib"
version := "1.0"
scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-compiler" %  "2.11.7",
    "org.scalatest" %% "scalatest" % "2.2.0" % "test" withSources(),
    "org.zeromq" % "jeromq" % "0.3.4",
    "org.apache.flink" %% "flink-dist" % "0.9-SNAPSHOT" % "provided" from "http://cloud01.prakinf.tu-ilmenau.de/flink-0.9.jar"
)

resolvers += "Sonatype (releases)" at "https://oss.sonatype.org/content/repositories/releases/"
