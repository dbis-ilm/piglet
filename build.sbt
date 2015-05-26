name := "PigParser"

lazy val commonSettings = Seq(
        version := "1.0",
        scalaVersion := "2.11.6",
        organization := "dbis"
)

// scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature","-Ylog-classpath")

val backNd = settingKey[String]("Backend Description")
backNd := sys.props.getOrElse("backNd",default= "flink")

lazy val print = taskKey[Unit]("Print out")
print := println(backNd.value)

//For String Template (Scalasti)
bintrayResolverSettings

/*
 * define the backend for the compiler: currently we support spark and flink
 */
val backend = "flink"

def backendlib(backend: String) = backend match {
    case "flink" => flinklib
    case "spark" => sparklib
}
def backendDependencies(backend: String) = backend match {
    case "flink" => "org.apache.flink" %% "flink-dist" % "0.9-SNAPSHOT" from "http://cloud01.prakinf.tu-ilmenau.de/flink-0.9.jar"
    case "spark" => "org.apache.spark" %% "spark-core" % "1.3.0"
}

excludeFilter in unmanagedSources := HiddenFileFilter || "*SparkCompileSpec.scala" || "*PigCompilerFlink.scala"

lazy val root = (project in file(".")).
  configs(IntegrationTest).
  settings(commonSettings: _*).
  settings(Defaults.itSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      "jline" % "jline" % "2.12.1",
      "org.scalatest" % "scalatest_2.11" % "2.2.0" % "test,it" withSources(),
      "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.3" withSources(),
      "org.scala-lang" % "scala-compiler" % "2.11.6",
      "com.assembla.scala-incubator" %% "graph-core" % "1.9.2",
      backendDependencies(backend),
      "org.apache.spark" %% "spark-core" % "1.3.0" % "provided", //DELETE Later
      "com.github.scopt" %% "scopt" % "3.3.0",
      "com.github.scala-incubator.io" % "scala-io-file_2.11" % "0.4.3-1",
      "org.clapper" %% "scalasti" % "2.0.0"
    )
  ).
  aggregate(backendlib(backend)).
  dependsOn(backendlib(backend))


lazy val sparklib = (project in file("sparklib")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      "org.scalatest" % "scalatest_2.11" % "2.2.0" % "test" withSources(),
      "org.scala-lang" % "scala-compiler" % "2.11.6",
      "org.apache.spark" %% "spark-core" % "1.3.0"
      // other settings
    )
  )

lazy val flinklib = (project in file("flinklib")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
    backendDependencies(backend)
    )
  )


mainClass in (Compile, packageBin) := Some("dbis.pig.PigREPL")

mainClass in (Compile, run) := Some("dbis.pig.PigCompiler")

assemblyJarName in assembly := "PigCompiler.jar"

test in assembly := {}

mainClass in assembly := Some("dbis.pig.PigCompiler")

assemblyMergeStrategy in assembly := {
    case PathList("scala", "util", xs @ _*)         => MergeStrategy.first
    case PathList(ps @ _*) if ps startsWith "scala" => MergeStrategy.first
    case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
}

//fork in run := true
