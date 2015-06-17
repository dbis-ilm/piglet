A = LOAD 'src/it/resources/file.csv' USING PigStorage(',') AS (f1:chararray, f2: int);
ZMQ_PUBLISHER A TO 'tcp://localhost:5556';
