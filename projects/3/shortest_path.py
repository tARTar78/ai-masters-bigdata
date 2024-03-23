import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import col, when

u = int(sys.argv[1])
v = int(sys.argv[2])
path = sys.argv[3]
output = sys.argv[4]

conf = SparkConf()
sc = SparkContext(appName="Pagerank", conf=conf)

spark = SparkSession.builder.config(conf=conf).appName("Spark SQL").getOrCreate()

graph_schema = StructType(fields=[
    StructField("user_id", LongType()),
    StructField("follower_id", LongType())
])

graph = spark.read.csv(path, sep="\t", schema=graph_schema)

max_length = 100
current_paths = spark.createDataFrame([(v,)], ["vertex_1"])
c = 1
while c != max_length and current_paths.count() > 0:
    current_paths = current_paths.join(graph, col("vertex_"+ str(c)) == col("follower_id"), "inner").withColumnRenamed("user_id", "vertex_"+str(c+1))
    current_paths = current_paths.drop("follower_id")
    if(current_paths[current_paths["vertex_" + str(c+1)] == u].count() > 0):
        current_paths = current_paths.filter(col("vertex_" + str(c+1)) == u)
        break
    c+=1

current_paths.write.csv(output, header=False)
spark.stop()
