A = LOAD 'src/it/resources/grouping.txt' AS (f1:int, f2:int, f3:int);
B = WINDOW A RANGE 5 SECONDS SLIDE RANGE 5 SECONDS;
C = GROUP B BY (f1,f2);
STORE C INTO 'grouping.out';
