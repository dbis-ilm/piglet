A = LOAD '$inbase/input/joinInput.csv' USING PigStorage(',') AS (a1:int,a2:int,a3:int);
B = LOAD '$inbase/input/joinInput.csv' USING PigStorage(',') AS (b1:int,b2:int,b3:int);

BF1 = FILTER B BY b1 == 4;
BF2 = FILTER B BY b3 == 1;

X = JOIN A BY a1, BF1 BY b1;
X2 = JOIN A BY a1, BF2 BY b1;

x0 = FOREACH X GENERATE A::a1, BF1::b1;
x1 = FOREACH X2 GENERATE A::a1, BF2::b1;
u = UNION x0, x1;

STORE u INTO '$outfile';
