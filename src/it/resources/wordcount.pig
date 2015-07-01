input = load 'src/it/resources/mary.txt' as (line);

words = foreach input generate flatten(TOKENIZE(line)) as word;

grpd = group words by word;

cntd = foreach grpd generate group, COUNT(words);

store cntd into 'marycounts.out';
