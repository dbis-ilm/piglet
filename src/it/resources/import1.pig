IMPORT 'src/it/resources/import2.pig';
B = FILTER A BY $0 > 10;
