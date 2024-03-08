ADD FILE predict.py;
FROM (
    SELECT * FROM hw2_test
    WHERE if1 > 20 AND if1 < 40
) o
INSERT INTO TABLE hw2_pred
SELECT TRANSFORM(*) FROM o
USING '/opt/conda/envs/dsenv/bin/python3 predict.py'
AS (id INT, prediction DOUBLE);

