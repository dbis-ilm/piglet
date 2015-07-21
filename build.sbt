name := "Piglet"

lazy val commonSettings = Seq(
  version := "1.0",
  scalaVersion := "2.11.7",
  organization := "dbis"
)


/*
 * define the backend for the compiler: currently we support spark and flink
 */
val backendEnv = sys.props.getOrElse("backend", default="spark")
//possibleBackends.contains(backendEnv) //TODO: outsource case _ => Exception part in all functions to here
val backends = settingKey[Map[String,Map[String,String]]]("Backend Settings")
backends := (backendEnv match {
  case "flink"      => flinkBackend
  case "spark"      => sparkBackend
  case _            => throw new Exception(s"Backend $backendEnv not available")
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
  libraryDependencies ++= Dependencies.rootDeps
).
settings(excludes(backendEnv): _*).
aggregate(backendlib(backendEnv).map(a => a.project): _*).
dependsOn(backendlib(backendEnv): _*)

lazy val common = project in file("common")

lazy val sparklib = (project in file("sparklib")).dependsOn(common)
lazy val flinklib = (project in file("flinklib")).dependsOn(common)


def backendlib(backend: String): List[ClasspathDep[ProjectReference]] = backend match {
  case "flink" => List(common, flinklib)
  case "spark" => List(common, sparklib)
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

// scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature","-Ylog-classpath")
testOptions in IntegrationTest += Tests.Argument("-oDF")
