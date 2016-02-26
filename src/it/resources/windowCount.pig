input = load '$inbase/input/mary.txt' using TextLoader() as (line);
words = foreach input generate flatten(TOKENIZE(line)) as word;
win = window words range 10 seconds slide range 10 seconds;
grpd = group win by word;
cntd = foreach grpd generate group, COUNT(win);
-- dump cntd;
store cntd into '$outfile';
