daily = load 'src/it/resources/nested.csv' using PigStorage(',') as (exchange, symbol);
win = window daily range 10 seconds slide range 10 seconds;
grpd  = group win by exchange;
uniqcnt  = foreach grpd {
           sym      = win.symbol;
           uniq_sym = distinct sym;
           generate group, COUNT(uniq_sym);
};
store uniqcnt into 'nested.out';
