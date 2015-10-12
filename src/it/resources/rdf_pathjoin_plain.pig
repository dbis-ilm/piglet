a = LOAD '$inbase/input/rdf-data.nt' using PigStorage(' ') as (subject: chararray, predicate: chararray, object:chararray);
b = BGP_FILTER a BY {
    ?book "<http://dbpedia.org/ontology/coverArtist>" ?artist .
    ?artist "<http://dbpedia.org/ontology/field>" ?field
    };
STORE b INTO '$outfile';