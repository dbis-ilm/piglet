name := "Piglet"

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

/*
 * For testing:
 */
lazy val print = taskKey[Unit]("Print out")
print := println(backends.value.get("default").get("name"))

/*
 * Main Project
 */
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
  libraryDependencies ++= Dependencies.rootDeps ++ backendDependencies(backendEnv),
  /* used for R integration */
  unmanagedJars in Compile += file("lib/jvmr_2.11-2.11.2.1.jar")
).
settings(excludes(backendEnv): _*).
aggregate(backendlib(backendEnv).map(a => a.project): _*).
dependsOn(backendlib(backendEnv): _*)

/*
 * Sub projects: supporting classes for Spark and Flink.
 */
lazy val sparklib = (project in file("sparklib")).
settings(commonSettings: _*).
settings(
  libraryDependencies ++= Dependencies.sparkDeps
)

lazy val flinklib = (project in file("flinklib")).
settings(commonSettings: _*).
settings(
  libraryDependencies ++= Dependencies.flinkDeps,
  resolvers += "Sonatype (releases)" at "https://oss.sonatype.org/content/repositories/releases/"
)

def backendlib(backend: String): List[ClasspathDep[ProjectReference]] = backend match {
  case "flink" => List(flinklib)
  case "spark" => List(sparklib)
  case "sparkflink" => List(sparklib, flinklib)
  case _ => throw new Exception(s"Backend $backend not available")
}

/*
 * Extra settings
 */
mainClass in (Compile, packageBin) := Some("dbis.pig.PigREPL")

mainClass in (Compile, run) := Some("dbis.pig.PigCompiler")

assemblyJarName in assembly := "PigCompiler.jar"

test in assembly := {}

mainClass in assembly := Some("dbis.pig.PigCompiler")
