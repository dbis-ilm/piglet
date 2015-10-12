a = LOAD '$inbase/input/rdf-data.nt' using PigStorage(' ') as (subject: chararray, predicate: chararray, object:chararray);
b = BGP_FILTER a BY {
    ?name "<http://dbpedia.org/ontology/author>" "<http://dbpedia.org/resource/Robert_Jordan>" .
    ?name "<http://dbpedia.org/ontology/author>" "<http://dbpedia.org/resource/Brandon_Sanderson>"
    };
STORE b INTO '$outfile';