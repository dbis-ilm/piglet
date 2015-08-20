A = LOAD 'src/it/resources/input/file.csv' USING PigStream(',') AS (f1:chararray, f2: int);
STORE A INTO 'result2.out';
