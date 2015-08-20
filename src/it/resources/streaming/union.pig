A = LOAD 'src/it/resources/input/file.csv' USING PigStream(',') AS (f1:int, f2: int);
B = LOAD 'src/it/resources/input/file.csv' USING PigStream(',') AS (f1:int, f2: int);
C = LOAD 'src/it/resources/input/file.csv' USING PigStream(',') AS (f1:int, f2: int);
D = UNION A, B, C;
STORE D INTO 'united.out';
