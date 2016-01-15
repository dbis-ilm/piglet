A  = LOAD '$inbase/input/matrix_data.csv' USING PigStorage(',') AS (v11: double, v12: double, v21: double, v22: double, v31: double, v32: double);
B = FOREACH A GENERATE ddmatrix(2, 3, {v11, v12, v21, v22, v31, v32});
STORE B INTO '$outfile';
