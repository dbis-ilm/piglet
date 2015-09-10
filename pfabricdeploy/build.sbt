import Dependencies._
name := "pfabricdeploy"

libraryDependencies ++= Seq(
    scalaCompiler,
    scalaTest % "test" withSources(),
    scalaLogging,
    typesafe,  
    scopt,
    "joda-time" % "joda-time" % "2.3",
    "org.apache.hadoop" % "hadoop-yarn-client" % "2.7.1" % "provided",
    "org.apache.hadoop" % "hadoop-common" % "2.7.1" % "provided",
    "org.apache.mesos" % "mesos" % "0.23.0",
    "org.eclipse.jetty" % "jetty-server" % "8.1.16.v20140903",
    "org.slf4j" % "slf4j-log4j12" % "1.7.6",
    "com.101tec" % "zkclient" % "0.6" ,
    "org.apache.kafka" % "kafka_2.10" % "0.8.2.1"
    
)

test in assembly := {}
assemblyJarName in assembly := "pfabric-deploy.jar"
