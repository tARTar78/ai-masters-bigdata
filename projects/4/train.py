from pyspark.sql import SparkSession
from pyspark.sql.types import *


spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

from model import pipeline
import sys


dataset_path = sys.argv[1]
model_path = sys.argv[2]

data = spark.read.json(dataset_path)
data = data.fillna("nothing", subset=["reviewText"])

pipeline_model = pipeline.fit(data)

pipeline_model.write().overwrite().save(model_path)
