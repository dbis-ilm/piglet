name := "Piglet"

lazy val commonSettings = Seq(
  version := "1.0",
  scalaVersion := "2.11.7",
  organization := "dbis"
)

// scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature","-Ylog-classpath")
testOptions in IntegrationTest += Tests.Argument("-oDF")

/*  
 * define the backend for the compiler: currently we support spark and flink
 */

val backendEnv = sys.props.getOrElse("backend", default="spark")
//possibleBackends.contains(backendEnv) //TODO: outsource case _ => Exception part in all functions to here
val backends = settingKey[Map[String,Map[String,String]]]("Backend Settings")
backends := (backendEnv match {
  case "flink"            => flinkBackend
  case "flinks"           => flinksBackend
  case "spark"            => sparkBackend
  case "sparks"           => sparksBackend
  case "scala"            => scalaBackend
  case _                  => throw new Exception(s"Backend $backendEnv not available")
})  

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
  libraryDependencies ++= Dependencies.rootDeps ++ backendDependencies(backendEnv)
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
).
settings(excludes(backendEnv): _*)

def backendlib(backend: String): List[ClasspathDep[ProjectReference]] = backend match {
  case "flink" | "flinks" => List(flinklib)
  case "spark" | "sparks" => List(sparklib)
  case "scala" => List(sparklib, flinklib)
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

EclipseKeys.skipParents in ThisBuild := false
