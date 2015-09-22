A = LOAD '$inbase/input/file.csv' USING PigStorage(',') AS (f1:int, f2: int);
B = SAMPLE A 1.0;
STORE B INTO '$outfile';
