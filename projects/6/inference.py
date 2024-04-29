from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

import joblib
import sys
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import FloatType
import pandas as pd

test_in = sys.argv[2]
pred_out = sys.argv[4]
sklearn_model_in = sys.argv[6]

#def predict_sentiment(model, features):
    # Предсказание настроения с использованием обученной модели и векторизованных признаков
#    return model.predict(features)

#spark = SparkSession.builder.getOrCreate()
test_data = spark.read.parquet(test_in)

# Загрузка обученной модели
model = joblib.load(sklearn_model_in)

# Применение обученной модели для предсказания настроения
#prediction_udf = spark.udf.register("predict_sentiment_udf", lambda features: predict_sentiment(model, features))
#predictions = test_data.withColumn("sentiment", prediction_udf("reviewText"))

@pandas_udf(returnType=FloatType())
def predict_sentiment(series):
    # Предсказание настроения с использованием обученной модели
    predictions = model.predict(series)
    return pd.Series(predictions)

# Применение функции предсказания к тестовым данным
predictions = test_data.withColumn("sentiment", predict_sentiment("reviewText"))

# Сохранение предсказаний в выходной файл
#predictions.write.option("header", True).csv("path/to/output/predictions.csv")
# Извлечение идентификаторов и предсказаний из DataFrame
predictions = predictions.select("id", "sentiment")

# Переименование столбца "sentiment" в "pred"
predictions = predictions.withColumnRenamed("sentiment", "pred")

# Сохранение предсказаний в выходной файл в формате "id,pred"
predictions.write.option("header", False).mode("overwrite").csv(pred_out)
predictions.write.option("header", False).mode("overwrite").csv("/users/tARTar78/my")

# Сохранение предсказаний в выходной файл
#predictions.write.option("header", False).mode("overwrite").csv(pred_out)
