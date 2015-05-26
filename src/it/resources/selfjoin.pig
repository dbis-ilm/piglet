A = LOAD "src/it/resources/file2.csv" USING PigStorage(",") AS (a1:int,a2:int,a3:int);
B = LOAD "src/it/resources/file2.csv" USING PigStorage(",") AS (b1:int,b2:int,b3:int);
X = JOIN A BY a1, B BY a1;
STORE X INTO "result2.out";
