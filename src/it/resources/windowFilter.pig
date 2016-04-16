A = LOAD '$inbase/input/file.csv' USING PigStream(',') AS (f1:int, f2: int);
B = WINDOW A RANGE 5 SECONDS SLIDE RANGE 5 SECONDS;
C = FILTER B BY f1>1 AND f2>1;
STORE C INTO '$outfile';
