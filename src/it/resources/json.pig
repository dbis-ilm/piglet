A = LOAD 'src/it/resources/input/file.json' USING JsonStorage() AS (address:(city:chararray, zipcode:chararray),name:chararray);
STORE A INTO 'json.out';
