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

There are several test cases included which should be passed: unit
tests can be executed by `sbt test`, integration tests which compile
and execute Pig scripts on Spark or Flink are executed by `sbt it:test`.

In order to support Pig functions and loaders an additional library `sparklib` is needed. This library can be build by

```
sbt 'project sparklib' package
```

Note that building the compiler requires the most recent Spark jars, but they will be downloaded by sbt automatically.
A similar library called `flinklib` is available for Flink. You can choose the backend which is supported by the Pig compiler
with an parameter for sbt: `-Dbackend=spark` or `-Dbackend=flink`. If
you want to use the compiler with the frontend scripts (see below),
you have to build an assembly:

```
sbt assembly
```

We provide a simple wrapper script for processing Pig scripts on Spark. Just call it with 

```
pigs --master local[4] your_script.pig
```

to compile the script and execute it on your local Spark
installation. A corresponding script called `pigf` exists for Flink.

In addition, there is an interactive Pig shell similar to Grunt:

```
pigsh -b spark
```

where Pig statements can be entered at the prompt and are executed as soon as
a `DUMP` or `STORE` statement is entered. Furthermore, the schema can be printed using `DESCRIBE`.
With the `-b` option you can specify which backend (spark, flink) will be used.

### Testing ###

We use the Scala testing framework as well as the scoverage tool for test coverage. You can produce
a coverage report by running `sbt clean coverage test`. The results can be found in 
`target/scala-2.11/scoverage-report/index.html`.

### ToDo ###

 * `COGROUP` and `GROUP BY` with multiple relations
 * `EXPLAIN` not implemented yet
