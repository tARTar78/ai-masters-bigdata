from sklearn.linear_model import LogisticRegression
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.pipeline import Pipeline
import joblib
import pandas as pd
import sys

train_in = sys.argv[2]
sklearn_model_out = sys.argv[4]

train_data = pd.read_parquet(train_in)
X_train = train_data.drop('label', axis=1)
y_train = train_data['label']

# Создание пайплайна
text_column = 'reviewText'
pipeline = Pipeline([
    ('vectorizer', CountVectorizer()),
    ('classifier', LogisticRegression())
])

# Обучение пайплайна
pipeline.fit(X_train[text_column], y_train)

# Сохранение обученной модели (пайплайна)
joblib.dump(pipeline, sklearn_model_out)

