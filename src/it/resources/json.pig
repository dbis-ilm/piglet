A = LOAD '$inbase/input/file.json' USING JsonStorage() AS (address:(city:chararray, zipcode:chararray),name:chararray);
STORE A INTO '$outfile';
