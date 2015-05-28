A = LOAD 'src/it/resources/file.csv' USING PigStorage(',') AS (f1:chararray, f2: int);
STORE A INTO 'result2.out';
