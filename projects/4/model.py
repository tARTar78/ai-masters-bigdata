from pyspark.ml.feature import Tokenizer, HashingTF, StringIndexer
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.sql.functions import col

tokenizer = Tokenizer(inputCol="reviewText", outputCol="words")

hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")

label_indexer = StringIndexer(inputCol="overall", outputCol="label")

lr = LinearRegression(featuresCol="features", labelCol="label")

pipeline = Pipeline(stages=[tokenizer, hashingTF, label_indexer, lr])
