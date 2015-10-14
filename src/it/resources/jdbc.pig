-- val df = sqlContext.load("jdbc",Map("driver"->"org.h2.Driver", "url"->"jdbc:h2:tcp://localhost/~/test", "user" -> "sa", "dbtable" -> "data"))
-- val df = sqlContext.load("jdbc",Map("driver"->"org.h2.Driver", "url"->"jdbc:sqlite:sqldb.db", "dbtable" -> "data"))

A = LOAD 'data' USING JdbcStorage('org.h2.Driver', 'jdbc:h2:file:$inbase/input/test?user=sa') AS (col1: int, col2:chararray);
STORE A INTO '$outfile';
