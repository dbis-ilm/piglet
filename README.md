## Pig Latin Compiler for Apache Spark / Flink ##

The goal of this project is to build a compiler for the Pig Latin dataflow language on modern data analytics
platforms such as Apache Spark and Apache Flink. The project is not intented as a replacement or competitor of
the official Pig compiler for Hadoop or its extensions such as PigSpork. Instead we have the following goals:

 * We want to build a compiler from scratch that compiles natively to the Scala-based Spark/Flink API and avoids all the
   stuff needed for MapReduce/Hadoop.
 * Though, we are aiming at being compatible to the original Pig compiler we plan to integrate extensiblity features
   allowing to define and use user-defined operators (not only UDFs) and in this way being able to integrate extensions
   for graph processing or machine learning.
 * Finally, it is also a nice exercise in Scala programming resulting in a more compact code simplifying maintenance
   and extensibility.

### Installation ###

#### Clone & Update ####
Simply clone the git project.


#### Build ####
To build the project, in the project directory invoke
```
sbt package
```

This will build the (main) Pig compiler project as well as the shipped backends.
(i.e. `sparklib` and `flinklib`)

There are several test cases included which should be passed: unit
tests can be executed by `sbt test`, integration tests which compile
and execute Pig scripts on Spark or Flink are executed by `sbt it:test`.

Note that building the compiler requires the most recent Spark and Flink jars, but they will be downloaded by sbt automatically.

If you want to use the compiler with the frontend scripts (see below),
you have to build an assembly:

```
sbt assembly
```

### Usage ###

We provide a simple wrapper script for processing Pig scripts. Just call it with

```
piglet --master local[4] --backend spark your_script.pig
```
To run this script you have to specify the full path of platform distribution jar in the environment 
variable `SPARK_JAR` for Spark (e.g. `spark-assembly-1.5.2-hadoop2.6.0.jar`) and in `FLINK_JAR` for Flink,  
e.g.

```
export SPARK_JAR=/opt/spark-1.5.2/assembly/target/scala-2.11/spark-assembly-1.5.2-hadoop2.6.0.jar
piglet --master local[4] --backend spark your_script.pig
```
Note, that both for Spark and Flink you need a version built for Scala 2.11 (see e.g. 
[Spark doc](http://spark.apache.org/docs/latest/building-spark.html#building-for-scala-211))
and for Flink you have to run the start script found in the bin directory (e.g. start-local.sh).

The following options are supported:
 * `--master m` specifies the master (local, yarn-client, yarn)
 * `--compile` compile and build jar, but do not execute
 * `--profiling` 
 * `--outdir dir` specifies the output directory for the generated code
 * `--backend b` specifies the backend to execute the script. Currently, we support 
    * `spark` (Apache Spark in batch mode)
    * `sparks`: Apache Spark Streaming
    * `flink`: Apache Flink in batch mode
    * `flinks`: Apache Flink Streaming
    * `mapreduce`: Apache Hadoop (by simply passing the script to the original Pig compiler)
 * `--backend_dir dir`
 * `--languages l` specifies the language dialects accepted by the parser: pig (standard Pig), sparql (pig + SPARQL extensions),
                  streaming (Pig + data stream extensions), cep (Pig + complex event processing extensions), all (all extensions)
 * `--params key=value, ...`
 * `--update-config`
 * `--show-plan` Print the resulting dataflow plan
 * `--show-stats` Show execution runtimes for (some) Piglet methods
 * `--keep` Keep generated files
 * `--sequential` If more than one input script is provided, do not merge them but execute them sequentially
 * `--log-level l`
 * `--backend-args key=value, ...` 

In addition, you can start an interactive Pig shell similar to Grunt:

```
piglet --interactive --backend spark
```

where Pig statements can be entered at the prompt and are executed as soon as
a `DUMP` or `STORE` statement is entered. Furthermore, the schema can be printed using `DESCRIBE`.

#### Docker ####

Piglet can also be run as a [Docker](https://www.docker.com/) container. However, the image is not 
yet on DockerHub, so it has to be built manually:
```
sbt clean package assembly
docker build -t dbis/piglet .
```

Currently, the Docker image supports the Spark backend only. 

To start the container, run:
```
docker run -it --rm --name piglet dbis/piglet 
```

This uses the container's entrypoint which runs piglet. The above command will print the help message.

You can start the interactive mode, using `-i` option and enter your script. 

```
docker run -it --rm --name piglet dbis/piglet -b spark -i
```

Alternatively, you can add your existing files into the container by [mounting volumes](https://docs.docker.com/engine/userguide/dockervolumes/#mount-a-host-file-as-a-data-volume) and run the script in batch mode:
```
docker run -it --rm --name piglet -v /tmp/test.pig:/test.pig dbis/piglet -b spark /test.pig
```

As mentioned before, the container provides an entrypoint that executes piglet. In case you need a bash for that container, 
you need to overwrite the entrypoint:
```
docker run -it --rm --name piglet --entrypoint /bin/bash dbis/piglet 
```

### Configuration ###

To configure the program, we ship a configuration file. When starting the program for the first time, we will create our program home directory in your home directory and also copy the configuration file into this directory.
More specifically, we will create a folder `~/.piglet` (on *nix like systems) and copy the configuration file `application.conf` to this location.

If you update Piglet to a new version and the configuration file still exists from a previous version, a configuration exception might occur because we cannot find new configuration keys introduced by the new Piglet version in the existing config file. In such cases, you can start piglet with the `-u` (`--update-config`) option. This will force the override of your old configuration (make sure you have a backup if needed). Alternatively, you can simply remove the existing `~/.piglet/application.conf`. This will also trigger the copy routine.

We use the [Typesafe Config](https://github.com/typesafehub/config/) library.

### Backends ###

As stated before, we support various backends that are used to execute the scripts. You can add your own backend by creating a jar file that contains the necessary configuration information and
classes and adding it to the classpath (e.g. using the `BACKEND_DIR` variable).

More detailed information on how to create backends can be found in [backends.md](backends.md)

### Further Information ###

 * Details on the supported language features (statements, functions, etc.) are described [here](Language.md).
 * Documentation on [how to setup](Zeppelin.md) integration with [Zeppelin](https://zeppelin.incubator.apache.org/).
 * We use the [Scala testing framework](http://www.scalatest.org/) as well as the [scoverage tool](http://scoverage.org/) 
   for test coverage. You can produce a coverage report by running `sbt clean coverage test`. The results can be found in
   `target/scala-2.11/scoverage-report/index.html`.
