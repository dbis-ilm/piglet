A = LOAD 'src/it/resources/joinInput.csv' USING PigStorage(',') AS (a1:int,a2:int,a3:int);
B = LOAD 'src/it/resources/joinInput.csv' USING PigStorage(',') AS (b1:int,b2:int,b3:int);
C = WINDOW A RANGE 10 seconds SLIDE RANGE 10 seconds;
D = WINDOW B RANGE 10 seconds SLIDE RANGE 10 seconds;
X = JOIN C BY a1, D BY b1;
STORE X INTO 'joinedW.out';
