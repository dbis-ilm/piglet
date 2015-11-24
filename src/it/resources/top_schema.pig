A = LOAD '$inbase/input/unsorted.csv' USING PigStorage(',') as (a: chararray, b: chararray, c: int);
B = order A by $0 asc, $2 desc;
C = limit B 4;
STORE C INTO '$outfile';
