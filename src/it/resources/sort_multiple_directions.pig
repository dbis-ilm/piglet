A = LOAD '$inbase/input/unsorted.csv' USING PigStorage(',') AS (f1:chararray, f2: chararray, f3: int);
B = ORDER A BY f1 asc, f2 desc;
STORE B INTO '$outfile';
