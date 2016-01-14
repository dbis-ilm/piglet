-- Based on: http://pig.apache.org/docs/r0.14.0/basic.html
A = LOAD '$inbase/input/split.csv' USING PigStream(',') AS (f1:int,f2:int,f3:int);
SPLIT A INTO X IF f1<7, Y IF f2==5, Z IF (f3<6 OR f3>6);
STORE X INTO '$outfile';
DUMP Y;
DUMP Z;
