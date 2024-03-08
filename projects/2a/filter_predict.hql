ADD FILE 2a.joblib;
ADD FILE projects/2a/predict.py;
ADD FILE projects/2a/model.py;
FROM (
    SELECT * FROM hw2_test
    WHERE if1 > 20 AND if1 < 40
) o
INSERT OVERWRITE DIRECTORY 'tARTar78_hw2_pred'
SELECT TRANSFORM(*) 
USING '/opt/conda/envs/dsenv/bin/python3 ./predict.py'
AS id, prediction;

