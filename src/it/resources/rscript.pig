A = LOAD 'src/it/resources/input/cluster-data.csv' USING PigStorage(',') AS (x: double, y: double);
B = RSCRIPT A USING 'library(fpc);db = dbscan($_, eps=.3, MinPts=5);cluster = cbind(inp, data.frame(db$cluster + 1L)); res = data.matrix(cluster)';
RES = FOREACH B GENERATE $0 AS x: double, $1 AS y: double, $2 AS cluster: int;
STORE RES INTO 'cluster.out';
