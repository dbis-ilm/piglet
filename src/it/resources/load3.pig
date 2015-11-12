a = load '$inbase/input/file.txt' using PigStorage(':');
b = filter a by $0 == "small";
store b into '$outfile';
