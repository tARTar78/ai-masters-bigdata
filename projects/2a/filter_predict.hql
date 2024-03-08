ADD FILE projects/2a/predict.py;

FROM (
    SELECT * FROM hw2_test
    WHERE if1 > 20 AND if1 < 40
) o
INSERT OVERWRITE DIRECTORY 'tARTar78_hw2_pred'
SELECT TRANSFORM(*) 
USING '/opt/conda/envs/dsenv/bin/python3 projects/2a/predict.py'
AS id, prediction;

