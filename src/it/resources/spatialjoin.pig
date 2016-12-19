REGISTER '/home/hage/code/stark/target/scala-2.11/stark.jar';

a1 = load '$inbase/input/events.csv' using PigStorage(',') as (name: chararray, lat: double, lon: chararray);
b1 = foreach a1 GENERATE  name, geometry("POINT("+lat+" "+lon+")") as loc1;

a2 = load '$inbase/input/events.csv' using PigStorage(',') as (name: chararray, lat: double, lon: chararray);
b2 = foreach a2 GENERATE  name, geometry("POINT("+lat+" "+lon+")") as loc2;

c = SPATIAL_JOIN b1, b2 ON containedby(loc1, loc2);

d = foreach c GENERATE b1::name, b2::name;

--DUMP d;
STORE d INTO '$outfile';
