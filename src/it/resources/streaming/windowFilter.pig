A = LOAD 'src/it/resources/input/file.csv' USING PigStream(',') AS (f1:int, f2: int);
B = WINDOW A ROWS  5 SLIDE ROWS 5;
C = FILTER B BY f1>1 AND f2>1;
STORE C INTO 'filtered.out';
