triples = LOAD 'file' AS (sub, pred, obj);
stmts = GROUP triples BY sub;
tmp = FOREACH stmts {
    r1 = FILTER triples BY (pred == 'aPred1');
    r2 = FILTER triples BY (pred == 'aPred2');
    GENERATE *, COUNT(r1) AS cnt1, COUNT(r2) AS cnt2;
};
DUMP tmp;
