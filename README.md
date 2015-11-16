## Pig Latin Compiler for Apache Spark / Flink ##

The goal of this project is to build a compiler for the Pig Latin dataflow language on modern data analytics
platforms such as Apache Spark and Apache Flink. The project is not intented as a replacement or competitor of
the official Pig compiler for Hadoop or its extensions such as PigSpork. Instead we have the following goals:

 * We want to build a compiler from scratch that compiles natively to the Scala-based Spark/Flink API and avoids all the
   stuff needed for MapReduce/Hadoop.
 * Though, we are aiming at being compatible to the original Pig compiler we plan to integrate extenisiblity features
   allowing to define and use user-defined operators (not only UDFs) and in this way being able to integrate extensions
   for graph processing or machine learning.
 * Finally, it is also a nice exercise in Scala programming resulting in a more compact code simplifying maintenance
   and extensibility.

### Installation & Usage ###

Simply clone the git project, change to the project directory and invoke

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

We provide a simple wrapper script for processing Pig scripts. Just call it with

```
piglet --master local[4] --backend spark your_script.pig
```

to compile the script and execute it on your local Spark
installation. The backend can be selected via the `--backend` option - currently
we support the following backends 
 * `spark`: Apache Spark in batch mode
 * `sparks`: Apache Spark Streaming
 * `flink`: Apache Flink in batch mode
 * `flinks`: Apache Flink Streaming
 * `mapreduce`: Apache Hadoop (by simply passing the script to the original Pig compiler)
 * `pipefabric`: the PipeFabric data stream engine
 * `storm`: Twitter Storm
   

In addition, you can start an interactive Pig shell similar to Grunt:

```
piglet --interactive --backend spark
```

where Pig statements can be entered at the prompt and are executed as soon as
a `DUMP` or `STORE` statement is entered. Furthermore, the schema can be printed using `DESCRIBE`.
With the `-b` option you can specify which backend (spark, flink) will be used.

### Testing ###

We use the Scala testing framework as well as the scoverage tool for test coverage. You can produce
a coverage report by running `sbt clean coverage test`. The results can be found in
`target/scala-2.11/scoverage-report/index.html`.

### Configuration ###

To configure the program, we ship a configuration file. When starting the program for the first time, we will create our program home directory in your home directory and also copy the configuration file into this directory.
More specifically, we will create a folder `~/.piglet` (on *nix like systems) and copy the configuration file `application.conf` to this location.

If you update Piglet to a new version and the configuration file still exists from a previous version, a configuration exception might occur because we cannot find new configuration keys introduced by the new Piglet version in the existing config file. In such cases, you can start piglet with the `-u` (`--update-config`) option. This will force the override of your old configuration (make sure you have a backup if needed). Alternatively, you can simply remove the existing `~/.piglet/application.conf`. This will also trigger the copy routine.

We use the [Typesafe Config](https://github.com/typesafehub/config/) library.

### Backends ###

As stated before, we support various backends that are used to execute the scripts. You can add your own backend by creating a jar file that contains the necessary configuration information and
classes and adding it to the classpath (e.g. using the `BACKEND_DIR` variable).

More detailed information on how to create backends can be found in [backends.md](backends.md)

### Supported Language Features ###

Depending on the target backend Piglet supports different language features. For batch processing in Spark and Flink we support the following standard Pig Latin statements:
 * LOAD
 * STORE
 * DUMP
 * FOREACH (including nested FOREACH)
 * GENERATE
 * FILTER
 * JOIN
 * CROSS
 * SPLIT INTO
 * DISTINCT
 * GROUP
 * UNION
 * LIMIT
 * SAMPLE
 * ORDER BY
 * STREAM
 * DEFINE (including macros)
 * REGISTER
 * SET
 
In addition to the standard Pig Latin statements we provide the following extensions:
 * RSCRIPT - sends data to a R script and converts the result back to a bag. Usage:
 
 ```
 out = RSCRIPT in USING '<R code>';
 ```
 Within the R code `$_` refers to the input data (a matrix), the result which will returned to the Piglet script has to be assigned to the R variable `res`.
 * ACCUMULATE - is used for incrementally calculating aggregates on (large) bags or streams of tuples.
 * MATERIALIZE - creates a materialization point, i.e. the bag is serialized into a HDFS file. Subsequent runs of the script (or other scripts sharing the same dataflow until the materialization point) can just start from this point. Usage:

```
MATERIALIZE bag;
``` 
 * embedded code - allows to embed Scala code for implementing user-defined functions and operators directly into the script. The code has to be enclosed by `<%` and `%>`. Usage:

```
<% def myFunc(i: Int): Int = i + 42 %>
out = FOREACH in GENERATE myFunc($0);
```
 
Furthermore, Piglet adds two statements simplifying the processing of RDF data:
 * RDFLOAD
 * TUPLIFY
 * BGP_FILTER
 
Finally, for processing streaming data using streaming backends (Flink Streaming, Spark Streaming, Storm, PipeFabric) we have added the following statements:
 * WINDOW
 * MATCH_EVENT - implements complex event processing. The statement supports the following clauses
    * PATTERN - defines the sequence of events, e.g. SEQ for a sequence, OR for alternative occurence, AND for mandatory occurence of both events, and NEG
      for the absence of an event 
    * WITH - describes the diffent events, e.g. (A: x == 0) means that the current tuple is detected as event A if x == 0
    * MODE
    * WITHIN - specifies the time frame for considering the sequence as a single event sequence
     
 * SOCKET_READ
 * SOCKET_WRITE
