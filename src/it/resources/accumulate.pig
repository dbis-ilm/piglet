A = LOAD '$inbase/input/file.csv' USING PigStorage(',') AS (f1: int, f2: int);
B = ACCUMULATE A GENERATE min(f1), max(f1), sum(f2), count(f2), avg(f2);
STORE B INTO '$outfile';
