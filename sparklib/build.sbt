name := "sparklib"
version := "1.0"
scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-compiler" %  "2.11.7",
    "org.scalatest" %% "scalatest" % "2.2.0" % "test" withSources(),
    "org.apache.spark" %% "spark-core" % "1.4.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "1.4.0" % "provided"
)
