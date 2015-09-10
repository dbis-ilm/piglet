A = LOAD '$inbase/input/file.csv' USING PigStorage(',') AS (f1:int, f2: int);
B = LOAD '$inbase/input/file.csv' USING PigStorage(',') AS (f1:int, f2: int);
C = LOAD '$inbase/input/file.csv' USING PigStorage(',') AS (f1:int, f2: int);
D = UNION A, B, C;
STORE D INTO '$outfile';
