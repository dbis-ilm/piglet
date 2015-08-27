a = load '$inbase/input/aggregate.csv' using PigStorage(',') as (x:int, y:int);
b = foreach a generate COUNT(y), SUM(y), AVG(y);
store b into '$outfile';
