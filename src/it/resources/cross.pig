A = LOAD '$inbase/input/file.txt' USING PigStorage(',') AS (f1:chararray);
B = LOAD '$inbase/input/file.txt' USING PigStorage(',') AS (f1:chararray);
--C = LOAD '$inbase/input/file.txt' USING PigStorage(',') AS (f1:int);
--D = CROSS A, B, C;
D = CROSS A,B;
STORE D INTO '$outfile';
