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
from model import preprocessor,fields


if __name__ == "__main__":
    # Read script arguments
    try:
        train_path = sys.argv[1]
        model_param1 = float(sys.argv[2])  # Преобразуем параметр во float
    except IndexError:
        logging.critical("Need to pass both train dataset path and model parameter")
        sys.exit(1)

    # Train the model
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
    ('linearregression', Ridge(alpha=model_param1))
    ])

    # Train linear regression model with Ridge regularization
    #model = Ridge(alpha=model_param1, normalize=True)  # Используем параметр alpha для управления регуляризацией
    model.fit(X_train, y_train)

    # Predict
    y_pred = model.predict(X_test)

    # Calculate mean squared error
    loss = log_loss(y_test, y_pred)

    # Logging parameters, metrics, and model using MLflow
    #existing_params = mlflow.active_run().data.params
    with mlflow.start_run():
        existing_params = mlflow.active_run().data.params
        if "model_param1" not in existing_params:
            mlflow.log_param("model_param1", model_param1)
        else:
            logging.warning("Parameter 'model_param1' already exists for this run. Skipping logging.")
        #mlflow.log_param("model_param1", model_param1, overwrite=True)
        #mlflow.log_param("train_path", train_path)
        mlflow.log_metric("log_loss", loss)
        mlflow.sklearn.log_model(model, "ridge_regression_model")

