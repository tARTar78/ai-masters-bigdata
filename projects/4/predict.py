from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import Pipeline, PipelineModel
import sys
import pyspark.sql.functions as f
from pyspark.sql.functions import when
from pyspark.sql.functions import col
from pyspark.sql.types import *

model_path = sys.argv[1]
test_path = sys.argv[2]
res_path = sys.argv[3]

model = PipelineModel.load(model_path)

data_schema = StructType([
    StructField("id", IntegerType()),
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

data = spark.read.json(test_path, schema = data_schema).cache()
#data = data.drop(
#  "reviewTime",
#  "reviewerID",
#  "asin",
#  "reviewerName",
#  "unixReviewTime")
data = data.fillna("0", subset=["reviewText"])
data = data.fillna("0", subset=["summary"])
#data = data.withColumn("comment_length", f.length(data.reviewText))
#data = data.fillna("0", subset=["vote"])
#data = data.withColumn("verified1", when(data["verified"] == "true", 1).otherwise(0))

predictions = model.transform(data)

predictions.write.mode("overwrite").save(res_path)
