a = load '$inbase/input/aggregate.csv' using PigStorage(',') as (x:int, y:int);
b = group a by x ;
c = foreach b generate group, COUNT(a.y), SUM(a.y), AVG(a.y);
store c into '$outfile';