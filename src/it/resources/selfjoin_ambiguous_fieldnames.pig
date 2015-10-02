A = LOAD '$inbase/input/joinInput.csv' USING PigStorage(',') AS (a1:int,a2:int,a3:int);
B = LOAD '$inbase/input/joinInput.csv' USING PigStorage(',') AS (a1:int,a2:int,a3:int);
X = JOIN A BY a1, B BY a1;
Y = ORDER X BY B::a1 ASC;
STORE Y INTO '$outfile';
