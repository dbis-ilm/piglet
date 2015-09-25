A = LOAD '$inbase/input/file.csv' USING PigStream(',') AS (f1:chararray, f2: int);
STORE A INTO '$outfile';
