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

data_schema = StructType([
    StructField("id", IntegerType()),
    StructField("overall", IntegerType()),
    StructField("vote", StringType()),
    StructField("verified", BooleanType()),
    StructField("reviewTime", StringType()),
    StructField("reviewerID", StringType()),
    StructField("asin", StringType()),
    StructField("reviewerName", StringType()),
    StructField("reviewText", StringType()),
    StructField("summary", StringType()),
    StructField("unixReviewTime", LongType()),
])

data = spark.read.json(dataset_path, schema = data_schema).cache()
#data = data.drop(
#  "reviewTime",
#  "reviewerID",
#  "asin",
#  "reviewerName",
#  "unixReviewTime")

data = data.fillna("0", subset=["reviewText"])
data = data.fillna("0", subset=["summary"])
#data = data.withColumn("comment_length", f.length(data.reviewText))
#data = data.withColumn("verified1", when(data["verified"] == "true", 1).otherwise(0))

pipeline_model = pipeline.fit(data)

pipeline_model.write().overwrite().save(model_path)
