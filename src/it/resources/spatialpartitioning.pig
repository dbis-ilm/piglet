a = load '$inbase/input/events.csv' using PigStorage(',') as (name: chararray, lat: double, lon: chararray);
b = foreach a GENERATE  name, geometry("POINT("+lat+" "+lon+")") as loc;
c = partition b on loc using grid(partitionsPerDimension=4, withExtent=false);
d = SPATIAL_FILTER c BY containedby(loc, geometry("POINT(50.1 10.2)"));
STORE d INTO '$outfile';
-- DUMP c;
