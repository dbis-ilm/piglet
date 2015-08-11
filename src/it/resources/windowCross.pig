A = LOAD 'src/it/resources/input/joinInput.csv' USING PigStorage(',') AS (a1:int,a2:int,a3:int);
B = LOAD 'src/it/resources/input/joinInput.csv' USING PigStorage(',') AS (b1:int,b2:int,b3:int);
C = WINDOW A RANGE 10 seconds SLIDE RANGE 10 seconds;
D = WINDOW B RANGE 10 seconds SLIDE RANGE 10 seconds;
X = CROSS C, D; 
STORE X INTO 'crossedW.out';
