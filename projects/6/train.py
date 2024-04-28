from sklearn.linear_model import LogisticRegression
import joblib
import pandas as pd
import sys


train_in = sys.argv[2]
sklearn_model_out = sys.argv[4]

train_data = pd.read_csv(train_in)
X_train = train_data.drop('label', axis=1)
y_train = train_data['label']
# Обучение логистической регрессии
classifier = LogisticRegression()
classifier.fit(X_train, y_train)
# Сохранение обученной модели
joblib.dump(classifier, sklearn_model_out)


