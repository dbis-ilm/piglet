name := "PigParser"

lazy val commonSettings = Seq(
        version := "1.0",
        scalaVersion := "2.11.6",
        organization := "dbis"
)

// scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature","-Ylog-classpath")

//For String Template (Scalasti)
bintrayResolverSettings

/*
 * define the backend for the compiler: currently we support spark and flink
 */
val backendEnv = sys.props.getOrElse("backend", default="flink")
val flinkSettings = Map(
    "name"    -> "flink",
    "class"   -> "dbis.pig.FlinkCompile")
val sparkSettings = Map(
    "name"    -> "spark",
    "class"   -> "dbis.pig.SparkCompile")
val flinkBackend = Map("flink" -> flinkSettings, "default" -> flinkSettings)
val sparkBackend = Map("spark" -> sparkSettings, "default" -> sparkSettings)
val flinksparkBackend = Map("flink" -> flinkSettings, "spark" -> sparkSettings, "default" -> sparkSettings)
val backends = settingKey[Map[String,Map[String,String]]]("Complex Backend Setting")
backends := (backendEnv match {
  case "flink"      => flinkBackend
  case "spark"      => sparkBackend
  case "sparkflink" => flinksparkBackend
  case _            => throw new Exception(s"Backend $backendEnv not available")
})


// For Testing:
lazy val print = taskKey[Unit]("Print out")
print := println(backends.value.get("default").get("name"))

def backendlib(backend: String) = backend match {
    case "flink" => flinklib
    case "spark" => sparklib
    case "sparkflink" => flinklib
    case _ => throw new Exception(s"Backend $backend not available")
}

def backendDependencies(backend: String) = backend match {
    case "flink" => "org.apache.flink" %% "flink-dist" % "0.9-SNAPSHOT" % "provided" from "http://cloud01.prakinf.tu-ilmenau.de/flink-0.9.jar"
    case "spark" => "org.apache.spark" %% "spark-core" % "1.3.0"
    case "sparkflink" => "org.apache.flink" %% "flink-dist" % "0.9-SNAPSHOT" % "provided" from "http://cloud01.prakinf.tu-ilmenau.de/flink-0.9.jar"
    case _ => throw new Exception(s"Backend $backend not available")
}

def excludes(backend: String) = backend match{
    case "flink" => {
        excludeFilter in unmanagedSources := 
            HiddenFileFilter            ||
            "*SparkCompileSpec.scala"   ||
            "*CompileSpec.scala"
    }
    case "spark" =>{
        excludeFilter in unmanagedSources := 
            HiddenFileFilter            || 
            "*FlinkCompileIt.scala"     ||
            "*FlinkCompileSpec.scala"   ||
            "*flink-template.stg"
    }
    case "sparkflink" =>{
      excludeFilter in unmanagedSources := 
            HiddenFileFilter            ||
            "*SparkCompileSpec.scala"   ||
            "*CompileSpec.scala"
    }
     case _ => throw new Exception(s"Backend $backend not available")

}
excludes(backendEnv)
  
lazy val root = (project in file(".")).
  configs(IntegrationTest).
  settings(commonSettings: _*).
  settings(Defaults.itSettings: _*).
  enablePlugins(BuildInfoPlugin).
  settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion, backends),
    buildInfoPackage := "dbis.pig",
    buildInfoObject := "BuildSettings"
  ).
  settings(
    libraryDependencies ++= Seq(
      "jline" % "jline" % "2.12.1",
      "org.scalatest" % "scalatest_2.11" % "2.2.0" % "test,it" withSources(),
      "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.3" withSources(),
      "org.scala-lang" % "scala-compiler" % "2.11.6",
      "com.assembla.scala-incubator" %% "graph-core" % "1.9.2",
      backendDependencies(backendEnv),
      "org.apache.spark" %% "spark-core" % "1.3.0" % "provided", //DELETE Later
      "com.github.scopt" %% "scopt" % "3.3.0",
      "com.github.scala-incubator.io" % "scala-io-file_2.11" % "0.4.3-1",
      "org.clapper" %% "scalasti" % "2.0.0"
    )
  ).
  aggregate(backendlib(backendEnv)).
  dependsOn(backendlib(backendEnv))


lazy val sparklib = (project in file("sparklib")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      "org.scalatest" % "scalatest_2.11" % "2.2.0" % "test" withSources(),
      "org.scala-lang" % "scala-compiler" % "2.11.6",
      backendDependencies(backendEnv)
      // other settings
    )
  )

lazy val flinklib = (project in file("flinklib")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
    "org.scalatest" % "scalatest_2.11" % "2.2.0" % "test" withSources(),
    "org.scala-lang" % "scala-compiler" % "2.11.6",
    backendDependencies(backendEnv)
    )
  )


mainClass in (Compile, packageBin) := Some("dbis.pig.PigREPL")

mainClass in (Compile, run) := Some("dbis.pig.PigCompiler")

assemblyJarName in assembly := "PigCompiler.jar"

test in assembly := {}

mainClass in assembly := Some("dbis.pig.PigCompiler")
