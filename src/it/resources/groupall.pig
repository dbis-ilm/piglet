A = LOAD '$inbase/input/grouping.txt' AS (f1:int, f2:int, f3:int);
B = GROUP A ALL;
STORE B INTO '$outfile';