A = LOAD '$inbase/input/file.json' USING JsonStorage() AS (address:(city:chararray, zipcode:chararray),name:chararray);
B = FOREACH A GENERATE address.city, address.zipcode, name;
STORE B INTO '$outfile';
