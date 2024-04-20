import os
import sys
import logging

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import Ridge  # Используем Ridge регрессию
from sklearn.metrics import log_loss
import mlflow
import mlflow.sklearn
from sklearn.pipeline import Pipeline
from model import preprocessor

def train_model(train_path, model_param1):
    # Logging initialization
    logging.basicConfig(level=logging.DEBUG)
    
    read_table_opts = dict(sep="\t", names=fields, index_col=False)
    df = pd.read_table(train_path, **read_table_opts)
    #split train/test
    X_train, X_test, y_train, y_test = train_test_split(
    df.drop(df.columns[1], axis=1), df.iloc[:,1], test_size=0.33, random_state=42
    )

    model = Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('linearregression', Ridge(alpha=model_param1, normalize=True))
    ])

    # Train linear regression model with Ridge regularization
    #model = Ridge(alpha=model_param1, normalize=True)  # Используем параметр alpha для управления регуляризацией
    model.fit(X_train, y_train)
    
    # Predict
    y_pred = model.predict(X_test)
    
    # Calculate mean squared error
    loss = log_loss(y_test, y_pred)
    
    # Logging parameters, metrics, and model using MLflow
    with mlflow.start_run():
        mlflow.log_param("model_param1", model_param1)
        mlflow.log_param("train_path", train_path)
        mlflow.log_metric("mean_squared_error", loss)
        mlflow.sklearn.log_model(model, "ridge_regression_model")

if __name__ == "__main__":
    # Read script arguments
    try:
        train_path = sys.argv[1]
        model_param1 = float(sys.argv[2])  # Преобразуем параметр во float
    except IndexError:
        logging.critical("Need to pass both train dataset path and model parameter")
        sys.exit(1)
    
    # Train the model
    train_model(train_path, model_param1)
