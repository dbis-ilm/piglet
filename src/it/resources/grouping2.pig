A = LOAD '$inbase/input/grouping.txt' AS (f1:int, f2:int, f3:int);
B = GROUP A BY f1;
C = FOREACH B GENERATE A.f1, AVG(A.f2);
STORE C INTO '$outfile';
