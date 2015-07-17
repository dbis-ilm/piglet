A = LOAD 'src/it/resources/file.json' USING JsonStorage() AS (address:(city:chararray, zipcode:chararray),name:chararray);
STORE A INTO 'json.out';
