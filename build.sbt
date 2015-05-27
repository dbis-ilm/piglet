name := "PigParser"

lazy val commonSettings = Seq(
        version := "1.0",
        scalaVersion := "2.11.6",
        organization := "dbis"
)

// scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature","-Ylog-classpath")

//For String Template (Scalasti)

/*  
 * define the backend for the compiler: currently we support spark and flink
 */
val backendEnv = sys.props.getOrElse("backend", default="flink")
val backends = settingKey[Map[String,Map[String,String]]]("Backend Settings")
backends := (backendEnv match {
  case "flink"      => flinkBackend
  case "spark"      => sparkBackend
  case "sparkflink" => flinksparkBackend
  case _            => throw new Exception(s"Backend $backendEnv not available")
})  


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
  libraryDependencies ++= Dependencies.rootDeps ++ backendDependencies(backendEnv),
  excludes(backendEnv)
).
aggregate(backendlib(backendEnv)).
dependsOn(backendlib(backendEnv))


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

def backendlib(backend: String) = backend match {
  case "flink" => flinklib
  case "spark" => sparklib
  case "sparkflink" => flinklib
  case _ => throw new Exception(s"Backend $backend not available")
}


//Extra Settings
mainClass in (Compile, packageBin) := Some("dbis.pig.PigREPL")

mainClass in (Compile, run) := Some("dbis.pig.PigCompiler")

assemblyJarName in assembly := "PigCompiler.jar"

test in assembly := {}

mainClass in assembly := Some("dbis.pig.PigCompiler")
