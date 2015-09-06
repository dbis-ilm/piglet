<%
def myFunc(i1: Int, i2: Int): Int = i1 + i2
%>
A = LOAD '$inbase/input/file.csv' USING PigStorage(',') AS (f1:int, f2: int);
B = FOREACH A GENERATE myFunc(f1, f2);
STORE B INTO '$outfile';
