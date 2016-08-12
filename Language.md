## Supported Language Features ##

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


In addition to the standard Pig Latin statements we provide several extensions described in the following.

### Matrix data types ###

Piglet supports matrices as builtin data type. For Scala-based backends the [Breeze library](https://github.com/scalanlp/breeze)
is used to implement this feature. A matrix field is specified by `[sd][di]matrix(rows, cols)` where the first
letter defines the representation (sparse or dense) and the second letter the element type (`double` or `int`), e.g.

```
m: dimatrix(10, 10)
```
for a dense 10x10 matrix of integer elements. In addition to this data type, a matrix constructor is provided that allows
to create a matrix from a bag of numbers. The constructor has the same notation as the matrix data type (prefix [sd][di]).
The following example creates a 2x2 matrix from 4 values:

```
out = FOREACH in GENERATE dimatrix(2, 2, $0, $1, $2, $3);
```

### R Integration ###

 * RSCRIPT - sends data to a R script and converts the result back to a bag. Usage:

 ```
 out = RSCRIPT in USING '<R code>';
 ```
 Within the R code `$_` refers to the input data (a matrix), the result which will returned to the Piglet
 script has to be assigned to the R variable `res`.

### ACCUMULATE ###

 * ACCUMULATE - is used for incrementally calculating aggregates on (large) bags or streams of tuples.

### MATERIALIZE ###

 * MATERIALIZE - creates a materialization point, i.e. the bag is serialized into a HDFS file. Subsequent
 runs of the script (or other scripts sharing the same dataflow until the materialization point) can just start from this point. Usage:

```
MATERIALIZE bag;
```

### Complex Event Processing ###

 * MATCH_EVENT - implements complex event processing. The statement supports the following clauses
    * PATTERN - defines the sequence of events, e.g. SEQ for a sequence, OR for alternative occurence, AND for mandatory occurence of both events, and NEG
      for the absence of an event
    * WITH - describes the different events, e.g. (A: x == 0) means that the current tuple is detected as event A if x == 0
    * MODE - ???
    * WITHIN - specifies the time frame for considering the sequence as a single event sequence


### Embedded Code ###

This allows to embed Scala code for implementing user-defined functions and operators directly into the script.
The code has to be enclosed by `<%` and `%>`. Usage:

```
<% def myFunc(i: Int): Int = i + 42 %>
out = FOREACH in GENERATE myFunc($0);
```

### SPARQL Integration ###

Furthermore, Piglet adds two statements simplifying the processing of RDF data:
 * RDFLOAD
 * TUPLIFY
 * BGP_FILTER

Using the SPARQL statements requires to run `piglet` with the `--language sparql` option.

### Stream Processing ###

Finally, for processing streaming data using streaming backends (Flink Streaming, Spark Streaming, Storm, PipeFabric)
we have added the following statements which can we used if you start `piglet` with the `--language streaming` option.

#### Window ####

#### Socket Read ####
The Socket Read operator allows to read from a data stream provided via socket connection.
The currently supported modes are standard (Inet-Sockets) and ZMQ Subscriber Sockets. The operators syntax looks as follows:
```sql
<A> = SOCKET_READ '<address>' [ MODE ZMQ ] [USING <StreamFunc>] [ AS <schema> ]
```
The mandatory part just needs an address of the form `IP/Hostname:Port`. When used with ZMQ mode the address
must be of the form as defined [here](http://api.zeromq.org/) and corresponding sub pages. The important part
here is the protocol prefix such as `tcp://`. With the USING part the user can state two functions:
 * PigStream(delimiter) - splits the input based on the provided delimiter character and transforms the tuples into a List.
 * RDFStream() - transforms the tuples to an Array of RDF form.

Without the USING part tab-separated tuples are expected. On top of that the user can enter a schema of the tuples.

#### Socket Write ####
With Socket Write you can stream the result data via socket connection. Similar like in Socket Read the two
supported modes are standard mode and ZMQ Publisher. The operator syntax is shown in the following:
```sql
SOCKET_WRITE <A> TO '<address>' [ MODE ZMQ ]
```
The mandatory part just needs a relation and an address of the form `IP/Hostname:Port`. When used with ZMQ
mode the address must be of the form as defined [here](http://api.zeromq.org/).

### Spatial Data ###

When using the spatial operations with Spark, you will need to include the "SpatialSpark" library. It can be included using the `REGISTER` statement:
```
REGISTER '/path/to/spatialspark.jar';
```
You may need according extensions for other platforms.

Geometries are supported as builtin types. A geometry type can be created using the `geometry(String)` constructor, which accepts a valud WKT string, e.g.:
```
g: geometry("POINT(1 1)")
```

To create points from a CSV(-like) input containing the x and y coordinates, you can create the string using inpug values of any type:
```
raw = LOAD '...' AS (id: chararray, x: double, y: chararray);
geoms = FOREACH raw GENERATE id, geometry("POINT("+x+" "+y+")") as geom;
```

#### Spatial Join ####
Two bags, `bag1` and `bag2` containing spatial fields as geometries, can be joined using spatial predicates:
```
out = SPATIALJOIN bag1, bag2 ON <predicate>(geom1, geom2);
```

where `geom1` is of type `geometry` in `bag1` and `geom2` is of type `geometry` in `bag2`.
Currently, two spatial predicates are supported:
  * contains - `geom1` contains `geom2`
  * intersects - `geom1` intersects with `geom2`

Please note, although we defined the behavior of these predicates here, the actual meaning is determined by the implementing backend.

#### Spatial Filter ####

Filtering on a spatial column can be achieved by using a spatial filter operator:
```
out = SPATIALFILTER in BY contains(geom, geometry("POLYGON(...)"));
```

*Note:* For spatial filter, the predicate is of the form `predicate(ref, geom-constructor)`, where
  
  - `predicate` is one of `intersects`, `contains`, `containedBy`
  - `ref` is a field reference in `in`
  - `geom-constructor` is an expression from which a geometry can be constructed

#### Indexing ####
The spatial spark extension supports indexed data sets. To create an index for a bag, simply use the `INDEX` operator:
```
out = INDEX in ON geom USING RTREE(...)
```
The `USING` method is for later use and disregarded currently!