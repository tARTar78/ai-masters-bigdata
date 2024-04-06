from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

from pyspark.sql.types import *
import pyspark.sql.functions as f
from model import pipeline
import sys
from pyspark.sql.functions import when

dataset_path = sys.argv[1]
model_path = sys.argv[2]

data = spark.read.json(dataset_path)

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

pipeline_model = pipeline.fit(data)

pipeline_model.write().overwrite().save(model_path)
