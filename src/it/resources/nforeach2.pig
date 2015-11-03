triples = RDFLOAD('$inbase/input/rdf-data.nt');
stmts = GROUP triples BY subject;
tmp = FOREACH stmts {
    r1 = FILTER triples BY (predicate == "<http://purl.org/dc/elements/1.1/date>");
    r2 = FILTER triples BY (predicate == "<http://purl.org/dc/elements/1.1/publisher>");
    GENERATE *, COUNT(r1) AS cnt1, COUNT(r2) AS cnt2;
};
STORE tmp INTO '$outfile';
