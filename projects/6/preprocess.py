from pyspark.ml.feature import Tokenizer, HashingTF, StringIndexer
#from pyspark.ml.regression import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.feature import *
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler

tokenizer1 = Tokenizer(inputCol="reviewText", outputCol="words1")
tokenizer2 = Tokenizer(inputCol="summary", outputCol="words2")

stop_words = StopWordsRemover.loadDefaultStopWords("english")
swr1 = StopWordsRemover(inputCol=tokenizer1.getOutputCol(), outputCol="words_filtered1", 
                       stopWords=stop_words)
swr2 = StopWordsRemover(inputCol=tokenizer2.getOutputCol(), outputCol="words_filtered2", 
                       stopWords=stop_words)

hashingTF1 = HashingTF(inputCol=swr1.getOutputCol(), outputCol="features1",numFeatures=100, binary=False)
hashingTF2 = HashingTF(inputCol=swr2.getOutputCol(), outputCol="features2",numFeatures=20, binary=False)

assembler = VectorAssembler(inputCols=["features1","features2"], outputCol="features")
#lr = LogisticRegression(featuresCol="features", labelCol="label",maxIter=15,regParam=0.01)

pipeline = Pipeline(stages=[tokenizer1,tokenizer2, swr1, swr2, hashingTF1, hashingTF2,assembler])
