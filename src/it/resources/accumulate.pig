A = LOAD '$inbase/input/file.csv' USING PigStorage(',') AS (f1: int, f2: int);
B = ACCUMULATE A GENERATE sum(f1);
-- min(f1), max(f1), sum(f2), count(f2), avg(f1);
STORE B INTO '$outfile';
