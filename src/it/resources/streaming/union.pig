A = LOAD '$inbase/input/file.csv' USING PigStream(',') AS (f1:int, f2: int);
B = LOAD '$inbase/input/file.csv' USING PigStream(',') AS (f1:int, f2: int);
C = LOAD '$inbase/input/file.csv' USING PigStream(',') AS (f1:int, f2: int);
D = UNION A, B, C;
STORE D INTO '$outfile';
