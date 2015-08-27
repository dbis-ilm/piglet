A = LOAD '$inbase/input/unsorted.csv' USING PigStorage(',') AS (f1:chararray, f2: chararray, f3: int);
B = ORDER A BY f1, f2, f3;
STORE B INTO '$outfile';
