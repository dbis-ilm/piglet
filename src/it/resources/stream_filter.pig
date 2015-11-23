A = LOAD '$inbase/input/file.csv' USING PigStream(',') AS (f1:int, f2: int);
B = FILTER A BY f1>1 AND f2>1;
STORE B INTO '$outfile';
