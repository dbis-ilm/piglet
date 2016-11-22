REGISTER '/home/hage/code/stark/target/scala-2.11/stark.jar';

a = load '$inbase/input/events.csv' using PigStorage(',') as (name: chararray, lat: chararray, lon: chararray);
b = foreach a GENERATE  name, geometry("POINT("+lat+" "+lon+")") as loc;
c = SPATIAL_FILTER b BY containedby(loc, geometry("POINT(50.1 10.2)"));
STORE c INTO '$outfile';
-- DUMP c;
