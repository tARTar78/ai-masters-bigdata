ADD FILE 2a.joblib;
ADD FILE projects/2a/predict.py;
ADD FILE projects/2a/model.py;
INSERT OVERWRITE TABLE hw2_pred
SELECT TRANSFORM (*)
USING "/opt/conda/envs/dsenv/bin/python ./predict.py"
AS id, prediction
FROM hw2_test
WHERE if1 > 20 AND if1 < 40;

