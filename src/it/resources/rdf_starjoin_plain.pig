a = LOAD '$inbase/input/sibdataset.nt' using PigStorage(' ') as (subject: chararray, predicate: chararray, object:chararray);
b = BGP_FILTER a BY {
    ?person "<http://xmlns.com/foaf/0.1/firstName>" ?f .
    ?person "<http://xmlns.com/foaf/0.1/lastName>" ?l
    };
STORE b INTO '$outfile';