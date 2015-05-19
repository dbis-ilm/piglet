name := "PigParser"

version := "1.0"

scalaVersion := "2.11.6"

//scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Ylog-classpath")

bintrayResolverSettings

val Dtarget = settingKey[String]("Dtarget")
Dtarget := sys.env.getOrElse("Dtarget", default = "flink")

libraryDependencies ++= Seq(
  "jline" % "jline" % "2.12.1",
  "org.scalatest" %% "scalatest" % "2.2.0" % "test" withSources(),
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.3" withSources(),
  "org.scala-lang" % "scala-compiler" % "2.11.6",
  "com.assembla.scala-incubator" %% "graph-core" % "1.9.2",
  "org.apache.spark" %% "spark-core" % "1.3.0" % "provided",
  "com.github.scopt" %% "scopt" % "3.3.0",
  "org.clapper" %% "scalasti" % "2.0.0"
)

mainClass in (Compile, packageBin) := Some("dbis.pig.PigREPL")

assemblyJarName in assembly := "PigCompiler.jar"

test in assembly := {}

mainClass in assembly := Some("dbis.pig.PigCompiler")

//fork in run := true
