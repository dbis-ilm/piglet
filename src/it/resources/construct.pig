data = load '$inbase/input/construct.csv' using PigStorage(',') as (f1: int, f2: int, name:chararray);
out = foreach data generate (f1, f2), {f1, f2}, [name, f1];
STORE out INTO '$outfile';
