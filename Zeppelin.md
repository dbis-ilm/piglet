## Zeppelin integration

We provide an integration with Apache Zeppelin - a web-bases notebook
for data analytics. It allows to write and execute Piglet scripts in
notebooks and visualize the results directly. For installation you
need
* the Zeppelin project from [here](https://zeppelin.incubator.apache.org/)
* Spark 1.5 built with Scala 2.11
* the zeppelin branch of Piglet

Setting up the Piglet interpreter requires the following steps:
1. Build the zeppelin interpreter with sbt:
```
sbt> package
sbt> assembly
sbt> project zeppelin
sbt> package
```

1. Copy the following Jar files to ZEPPELIN_HOME/interpreter/piglet
  * PIGLET_HOME/common/target/scala-2.11/common_2.11-0.3.jar
  * PIGLET_HOME/sparklib/target/scala-2.11/sparklib_2.11-0.3.jar
  * PIGLET_HOME/target/scala-2.11/PigCompiler.jar
  * PIGLET_HOME/zeppelin/target/scala-2.11/piglet-interpreter_2.11-0.3.jar
  * spark-assembly-1.5.2-hadoop2.6.0.jar

1. Add the Piglet interpreter in ZEPPELIN_HOME/conf/zeppelin-site.xml
by adding `dbis.piglet.PigletInterpreter` to the property value
`zeppelin.interpreters`.

1. Create a new notebook, go to "Interpreter binding", and activate
   `piglet %piglet`.

1. Enter your script and mark it as Piglet using `%piglet`.

1. Note that you have to use `DISPLAY relation` instead of `DUMP` to visualize the result.
