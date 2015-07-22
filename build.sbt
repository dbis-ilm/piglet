name := "Piglet"

libraryDependencies ++= Dependencies.rootDeps

mainClass in (Compile, packageBin) := Some("dbis.pig.PigREPL")

mainClass in (Compile, run) := Some("dbis.pig.PigCompiler")

assemblyJarName in assembly := "PigCompiler.jar"

mainClass in assembly := Some("dbis.pig.PigCompiler")

test in assembly := {}

testOptions in IntegrationTest += Tests.Argument("-oDF")

// scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature","-Ylog-classpath")
