name := "PigParser"

lazy val commonSettings = Seq(
  version := "1.0",
  scalaVersion := "2.11.6",
  organization := "dbis"
)

// scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature","-Ylog-classpath")


/*  
 * define the backend for the compiler: currently we support spark and flink
 */

val backendEnv = sys.props.getOrElse("backend", default="spark")
//possibleBackends.contains(backendEnv) //TODO: outsource case _ => Exception part in all functions to here
val backends = settingKey[Map[String,Map[String,String]]]("Backend Settings")
backends := (backendEnv match {
  case "flink"      => flinkBackend
  case "spark"      => sparkBackend
  case "sparkflink" => flinksparkBackend
  case _            => throw new Exception(s"Backend $backendEnv not available")
})  
// For Testing:
lazy val print = taskKey[Unit]("Print out")
print := println(backends.value.get("default").get("name"))



//Main Project
lazy val root = (project in file(".")).
configs(IntegrationTest).
settings(commonSettings: _*).
settings(Defaults.itSettings: _*).
enablePlugins(BuildInfoPlugin).
settings(
  buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion, backends),
  buildInfoPackage := "dbis.pig",
  buildInfoObject := "BuildSettings",
  bintrayResolverSettings,
  libraryDependencies ++= Dependencies.rootDeps ++ backendDependencies(backendEnv)
).
settings(excludes(backendEnv): _*).
aggregate(backendlib(backendEnv).map(a => a.project): _*).
dependsOn(backendlib(backendEnv): _*).
aggregate(eventlib).
dependsOn(eventlib)

//Sub-Projects
lazy val sparklib = (project in file("sparklib")).
settings(commonSettings: _*).
settings(
  libraryDependencies ++= Dependencies.sparkDeps
)

lazy val flinklib = (project in file("flinklib")).
settings(commonSettings: _*).
settings(
  libraryDependencies ++= Dependencies.flinkDeps
)

lazy val eventlib = (project in file("eventlib")).
settings(commonSettings: _*).
settings(
  libraryDependencies ++= Seq(
    "org.scalatest" % "scalatest_2.11" % "2.2.0" % "test" withSources(),
    "org.scala-lang" % "scala-compiler" % "2.11.6",
    "org.apache.spark" %% "spark-core" % "1.3.0" % "provided"
    // other settings
  )
)

def backendlib(backend: String): List[ClasspathDep[ProjectReference]] = backend match {
  case "flink" => List(flinklib)
  case "spark" => List(sparklib)
  case "sparkflink" => List(sparklib, flinklib)
  case _ => throw new Exception(s"Backend $backend not available")
}

//Extra Settings
mainClass in (Compile, packageBin) := Some("dbis.pig.PigREPL")

mainClass in (Compile, run) := Some("dbis.pig.PigCompiler")

assemblyJarName in assembly := "PigCompiler.jar"

test in assembly := {}

mainClass in assembly := Some("dbis.pig.PigCompiler")
