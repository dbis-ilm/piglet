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

There are several test cases included which should be passed: unit tests can be executed by `sbt test`, integration tests which compile and execute Pig scripts on Spark are executed by `sbt it:test`.

In order to support Pig functions and loaders an additional library `sparklib` is needed. This library can be build by

```
sbt 'project sparklib' package
```

Note that building the compiler requires the most recent Spark jars, but they will be downloaded by sbt automatically.


We provide a simple wrapper script for processing Pig scripts. Just call it with 

```
pigs --master local[4] your_script.pig
```

to compile the script and execute it on your local Spark installation.

In addition, there is an interactive Pig shell similar to grunt:

```
pigsh
```

where Pig statements can be entered at the prompt and are executed as soon as
a `DUMP` or `STORE` statement is entered. Furthermore, the schema can be printed using `DESCRIBE`.


### Mapping of Pig statements to Spark ###

Currently, we have implemented to following mappings in the code generator.

| Pig statement  | Spark code |
| ------------- | ------------- |
| `LOAD "file" USING storage-func() AS schema-def` | `sc.textFile(file)` or `storage-func.load(sc, file)`  |
| `DUMP alias` |  `alias.collect.map(t => println(t.mkString(","))` |
| `STORE alias INTO "file"` |  `alias.coalesce(1, true).saveAsTextFile(file)` |
| `FILTER alias BY predicate`  | `alias.filter(t => predicate(t))`   |
| `FOREACH alias GENERATE`  |    |
| `DISTINCT alias` |  `alias.distinct` |
| `LIMIT alias num` |  `sc.parallelize(alias.take(num))` |
| `UNION alias1, alias2, ...` | `alias1.union(alias2.union(...))` |
| `SAMPLE alias size` |  `alias.sample(size)` |
| `ORDER alias1 BY field1 ASC, field2 ASC ...` |  |
| `JOIN alias1 BY expr1, alias2 BY expr2, ...` |  |
| `GROUP alias ALL`| `alias.glom`  |
| `GROUPB alias BY expr` | `alias.groupBy(t => {expr}).map{case (k,v) => List(k,v)}`|
| `STREAM alias THROUGH op(params)` |  `op(alias, params)` |

### Testing ###

We use the Scala testing framework as well as the scoverage tool for test coverage. You can produce
a coverage report by running `sbt clean coverage test`. The results can be found in 
`target/scala-2.11/scoverage-report/index.html`.

### ToDo ###

 * Flink not supported yet; requires a scala 2.11 build of Flink
 * nested blocks in `FOREACH`
 * `COGROUP` and `GROUP BY` with multiple relations
 * UDF support
 * `EXPLAIN` not implemented yet
