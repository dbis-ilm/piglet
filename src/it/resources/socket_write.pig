A = LOAD '$inbase/input/file.csv' USING PigStream(',') AS (f1:chararray,f2:int);
SOCKET_WRITE A TO 'localhost:9999';
