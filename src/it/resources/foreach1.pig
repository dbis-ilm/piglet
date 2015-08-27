A = LOAD '$inbase/input/events.csv' USING PigStorage(',') AS (id:chararray, longitude: double, latitude: double);
B = FOREACH A GENERATE id, longitude + 0.01, latitude + 3.5;
STORE B INTO '$outfile';