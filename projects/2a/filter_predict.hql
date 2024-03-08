ADD FILE predict.py;
FROM (
    SELECT * FROM tARTar78.hw2_test
    WHERE if1 > 20 AND if1 < 40
) o
INSERT INTO TABLE tARTAr78.hw2_pred
SELECT TRANSFORM(*) FROM o
USING '/opt/conda/envs/dsenv/bin/python3 predict.py'
AS (id INT, prediction DOUBLE);

