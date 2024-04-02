from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import Pipeline, PipelineModel
import sys

model_path = sys.argv[1]
test_path = sys.argv[2]
res_path = sys.argv[3]

model = PipelineModel.load(model_path)

data = spark.read.json(test_path)

data = data.fillna("nothing", subset=["reviewText"])

predictions = model.transform(data)

predictions.write.mode("overwrite").save(res_path)
