A = LOAD '$inbase/input/file.csv' USING PigStorage(',') AS (f1:chararray, f2: int);
B = LOAD '$inbase/input/file.csv' USING PigStorage(',') AS (f1:int, f2: int);
C = LOAD '$inbase/input/file.csv' USING PigStorage(',') AS (f1:int, f2: chararray);
D = LOAD '$inbase/input/file.txt' USING PigStorage(',') AS (f1: chararray);
E = CROSS A, B, C, D;
STORE E INTO '$outfile';
-- DUMP E;
