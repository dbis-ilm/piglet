A = LOAD '$inbase/input/grouping.txt' USING PigStream('\t') AS (f1:int, f2:int, f3:int);
B = WINDOW A RANGE 5 SECONDS SLIDE RANGE 5 SECONDS;
C = GROUP B BY (f1,f2);
STORE C INTO '$outfile';
