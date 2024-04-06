from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import Pipeline, PipelineModel
import sys
import pyspark.sql.functions as f
from pyspark.sql.functions import when
from pyspark.sql.functions import col

model_path = sys.argv[1]
test_path = sys.argv[2]
res_path = sys.argv[3]

model = PipelineModel.load(model_path)

data = spark.read.json(test_path)

data = data.drop(
  "reviewTime",
  "reviewerID",
  "asin",
  "reviewerName",
  "unixReviewTime",
"summary")

data = data.fillna("nothing", subset=["reviewText"])
data = data.withColumn("comment_length", f.length(data.reviewText))
data = data.fillna("0", subset=["vote"])
data = data.withColumn("verified1", when(data["verified"] == "true", 1).otherwise(0))

predictions = model.transform(data)

predictions.write.mode("overwrite").save(res_path)
