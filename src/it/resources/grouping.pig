A = LOAD '$inbase/input/grouping.txt' AS (f1:int, f2:int, f3:int);
B = GROUP A BY (f1,f2);
STORE B INTO '$outfile';
