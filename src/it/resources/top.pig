A = LOAD '$inbase/input/unsorted.csv' USING PigStorage(',');
B = order A by $0 asc, $2 desc;
C = limit B 4;
STORE C INTO '$outfile';
