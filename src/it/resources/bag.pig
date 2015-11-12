A  = LOAD '$inbase/input/mary.txt' AS (f1:chararray);
X = FOREACH A GENERATE TOKENIZE(f1);
STORE X INTO '$outfile';
