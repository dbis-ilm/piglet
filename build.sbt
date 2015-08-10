name := "Piglet"

libraryDependencies ++= Dependencies.rootDeps

mainClass in (Compile, packageBin) := Some("dbis.pig.PigREPL")

mainClass in (Compile, run) := Some("dbis.pig.PigCompiler")

assemblyJarName in assembly := "PigCompiler.jar"

test in assembly := {}

mainClass in assembly := Some("dbis.pig.PigCompiler")

testOptions in IntegrationTest += Tests.Argument("-oDF")

// scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature","-Ylog-classpath")

//excludeFilter in (IntegrationTest, unmanagedSources) := HiddenFileFilter || itExludes
testOptions in IntegrationTest := Seq(Tests.Filter(s => {println(s); itTests.contains(s)}))