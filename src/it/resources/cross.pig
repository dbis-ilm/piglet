A = LOAD '$inbase/input/file.txt' USING PigStorage(',') AS (f1:chararray); --, f2:int
B = LOAD '$inbase/input/file.txt' USING PigStorage(',') AS (f1:chararray);
D = CROSS A,B;
STORE D INTO '$outfile';
-- DUMP D;
