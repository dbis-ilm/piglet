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
 * MATCH_EVENT - implements complex event processing. The statement supports the following clauses
    * PATTERN - defines the sequence of events, e.g. SEQ for a sequence, OR for alternative occurence, AND for mandatory occurence of both events, and NEG
      for the absence of an event
    * WITH - describes the different events, e.g. (A: x == 0) means that the current tuple is detected as event A if x == 0
    * MODE - ???
    * WITHIN - specifies the time frame for considering the sequence as a single event sequence

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
 * SOCKET_READ
 * SOCKET_WRITE