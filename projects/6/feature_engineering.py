from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

from pyspark.sql.types import *
import pyspark.sql.functions as f
from preprocess import pipeline
import sys
from pyspark.sql.functions import when

dataset_path = sys.argv[2]
model_path = sys.argv[4]

#data_schema = StructType([
#    StructField("id", IntegerType()),
#    StructField("label", IntegerType()),
#    StructField("vote", StringType()),
#    StructField("verified", BooleanType()),
#    StructField("reviewTime", StringType()),
#    StructField("reviewerID", StringType()),
#    StructField("asin", StringType()),
#    StructField("reviewerName", StringType()),
#    StructField("reviewText", StringType()),
#    StructField("summary", StringType()),
#    StructField("unixReviewTime", LongType()),
#])

data = spark.read.json(dataset_path).cache()
data = data.drop(
  "reviewTime",
  "reviewerID",
  "asin",
  "reviewerName",
  "unixReviewTime",
  "vote",
  "verified",
  "summary")

data = data.fillna("0", subset=["reviewText"])
#data = data.fillna("0", subset=["summary"])
data = data.withColumn("comment_length", f.length(data.reviewText))
#data = data.withColumn("verified1", when(data["verified"] == "true", 1).otherwise(0))

#processed_data = pipeline.transform(data)
#processed_data.write.mode("overwrite").parquet(model_path)

#pipeline_model = pipeline.fit(data)
#processed_data = pipeline_model.transform(data)

data.write.mode("overwrite").parquet(model_path)

#pipeline_model = pipeline.fit(data)

#pipeline_model.write().overwrite().save(model_path)

