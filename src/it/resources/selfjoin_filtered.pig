A = LOAD '$inbase/input/joinInput.csv' USING PigStorage(',') AS (a1:int,a2:int,a3:int);
B = LOAD '$inbase/input/joinInput.csv' USING PigStorage(',') AS (b1:int,b2:int,b3:int);
X = JOIN A BY a1, B BY b1;
Y = FILTER X BY a1 == 4;
STORE Y INTO '$outfile';
