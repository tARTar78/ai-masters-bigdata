from pyspark.ml.feature import Tokenizer, HashingTF, StringIndexer
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.feature import *
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler

tokenizer = Tokenizer(inputCol="reviewText", outputCol="words")

stop_words = StopWordsRemover.loadDefaultStopWords("english")
swr = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="words_filtered", stopWords=stop_words)

hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")

label_indexer = StringIndexer(inputCol="overall", outputCol="label")

vote_indexer = StringIndexer(inputCol="vote", outputCol="vote1")

assembler = VectorAssembler(inputCols=["features", "comment_length","vote1","verified1"], outputCol="finalfeatures")

lr = LinearRegression(featuresCol="finalfeatures", labelCol="label")

pipeline = Pipeline(stages=[tokenizer,swr, hashingTF, vote_indexer, label_indexer, assembler, lr])
