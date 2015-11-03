-- triples = LOAD '$inbase/input/rdf-data.nt' USING RDFFileStorage AS (subject: chararray, predicate: chararray, object: chararray);
triples = RDFLOAD('$inbase/input/rdf-data.nt');
stmts = GROUP triples BY subject;
tmp = FOREACH stmts GENERATE *;
STORE tmp INTO '$outfile';
