a = LOAD '$inbase/input/sibdataset.nt' using PigStorage(' ') as (subject: chararray, predicate: chararray, object:chararray);
b = BGP_FILTER a BY {
    ?user "<http://rdfs.org/sioc/ns#account_of>" ?person
};
STORE b INTO '$outfile';