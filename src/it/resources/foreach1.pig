REGISTER "eventlib/target/scala-2.11/eventlib_2.11-1.0.jar";
A = LOAD 'src/it/resources/events.csv' USING PigStorage(',') AS (id:chararray, longitude: double, latitude: double);
B = FOREACH A GENERATE id, dbis.events.Distances.spatialDistance(longitude, latitude, 50.0, 10.0) AS dist: double;
STORE B INTO 'distances.out';