A = LOAD 'data' USING JdbcStorage('org.h2.Driver', 'jdbc:h2:file:$inbase/input/test?user=sa&ACCESS_MODE_DATA=r') AS (col1: int, col2:chararray);
STORE A INTO '$outfile';
