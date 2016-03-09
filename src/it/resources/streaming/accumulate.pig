A = LOAD '$inbase/input/file.csv' USING PigStream(',') AS (f1: int, f2: int);
B = GROUP A BY f1;
C = ACCUMULATE B GENERATE min(A.f1), max(A.f1), sum(A.f2), count(A.f2), avg(A.f2);
STORE C INTO '$outfile';
