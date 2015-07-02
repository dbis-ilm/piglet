-- Load input from the file named mary.txt and call the single
-- field in the record 'line'.
input = load 'src/it/resources/mary.txt' as (line);

-- TOKENIZE splits the line into a field for each word.
-- flatten will take the collection of records returned b
-- TOKENIZE and produce a separate record for each one, calling the single
-- field in the record word.
words = foreach input generate flatten(TOKENIZE(line)) as word;

-- Now group them together by each word.
grpd = group words by word;

-- Count them.
cntd = foreach grpd generate group, COUNT(words);

store cntd into 'marycounts.out';
