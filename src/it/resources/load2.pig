A = LOAD '$inbase/input/file.csv' USING PigStorage(',') AS (f1:chararray, f2: int);
STORE A INTO '$outfile';
