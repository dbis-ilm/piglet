daily = load 'src/it/resources/nested.csv' using PigStorage(',') as (exchange, symbol);
grpd  = group daily by exchange;
uniqcnt  = foreach grpd {
           sym      = daily.symbol;
           uniq_sym = distinct sym;
           generate group, COUNT(uniq_sym);
};
store uniqcnt into 'nested.out';