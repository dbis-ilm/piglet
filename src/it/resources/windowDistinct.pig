A = LOAD 'src/it/resources/duplicates.csv' USING PigStorage(',') AS (f1:int, f2: int);
B = WINDOW A RANGE 10 SECONDS SLIDE RANGE 10 SECONDS;
C = DISTINCT B;
STORE C INTO 'unique.out';
