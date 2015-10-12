DEFINE my_macro(in_alias, p) RETURNS out_alias {
    $out_alias = FOREACH $in_alias GENERATE $0 + $p, $1;
};

DEFINE my_macro2(in_alias, p) RETURNS out_alias {
    $out_alias = FOREACH $in_alias GENERATE $0, $1 - $p;
};

in = LOAD '$inbase/input/file.csv' USING PigStorage(',') AS (f1:int, f2: int);
out = my_macro(in, 42);
out2 = my_macro2(out, 1);

STORE out2 INTO '$outfile';
