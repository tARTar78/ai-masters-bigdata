from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

import joblib
import sys

test_in = sys.argv[2]
pred_out = sys.argv[4]
sklearn_model_in = sys.argv[6]

def predict_sentiment(model, features):
    # Предсказание настроения с использованием обученной модели и векторизованных признаков
    return model.predict(features)

#spark = SparkSession.builder.getOrCreate()
test_data = spark.read.parquet(test_in)

# Загрузка обученной модели
model = joblib.load(sklearn_model_in)

# Применение обученной модели для предсказания настроения
prediction_udf = spark.udf.register("predict_sentiment_udf", lambda features: predict_sentiment(model, features))
predictions = test_data.withColumn("sentiment", prediction_udf("features"))

# Сохранение предсказаний в выходной файл
predictions.write.option("header", False).csv(pred_out)
