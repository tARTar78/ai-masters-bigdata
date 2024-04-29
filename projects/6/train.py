from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
import joblib
import pandas as pd
import sys

train_in = sys.argv[2]
sklearn_model_out = sys.argv[4]

train_data = pd.read_parquet(train_in)
X_train = train_data.drop('label', axis=1)
y_train = train_data['label']

# Define the preprocessing steps
preprocessor = Pipeline([
    ('categorical', OneHotEncoder()),
    ('numeric', StandardScaler())
])

# Define the full pipeline
classifier = Pipeline([
    ('preprocess', preprocessor),
    ('model', LogisticRegression())
])

# Fit the pipeline to the data
classifier.fit(X_train, y_train)

# Save the trained model
joblib.dump(classifier, sklearn_model_out)
