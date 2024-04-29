from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from datetime import datetime

spark_binary1 = '/usr/bin/spark3-submit'

with DAG(dag_id='tARTar78_dag', start_date=datetime(2024, 5, 27), schedule_interval=None, catchup=False) as dag:
    #base_dir = '{{ dag_run.conf["base_dir"] if dag_run else "" }}'
    base_dir = '{{ dag_run.conf.get("base_dir", "") }}'
    feature_eng_train_task = SparkSubmitOperator(
        task_id='feature_eng_train_task',
        conn_id='spark_default',
        application=f'{base_dir}/feature_engineering.py',
        application_args=['--train-in', '/datasets/amazon/amazon_extrasmall_train.json', '--train-out', 'tARTar78_train_out'],
        env_vars={
            'PYSPARK_PYTHON': '/opt/conda/envs/dsenv/bin/python'
        },
        spark_binary=spark_binary1,
	num_executors = 4,
	executor_cores=1,
	executor_memory="2G"
    )

    download_train_task = BashOperator(
        task_id='download_train_task',
        bash_command=f'hdfs dfs -get tARTar78_train_out {base_dir}/tARTar78_train_out_local'
    )

    train_task = BashOperator(
        task_id='train_task',
        bash_command=f'/opt/conda/envs/dsenv/bin/python {base_dir}/train.py --train-in {base_dir}/tARTar78_train_out_local --model-out {base_dir}/6.joblib'
    )

    model_path = f"{base_dir}/6.joblib"
    model_sensor = FileSensor(
        task_id='model_sensor',
        fs_conn_id='hdfs_default',
        filepath=model_path,
        timeout=5 * 60,  # 5 minutes
        poke_interval=10  # Check every 10 seconds
    )

    feature_eng_test_task = SparkSubmitOperator(
        task_id='feature_eng_test_task',
        conn_id='spark_default',
        application=f'{base_dir}/feature_engineering.py',
        application_args=['--test-in', '/datasets/amazon/amazon_extrasmall_test.json', '--test-out', 'tARTar78_test_out'],
        env_vars={
            'PYSPARK_PYTHON': '/opt/conda/envs/dsenv/bin/python'
        },
        spark_binary=spark_binary1,
        num_executors = 4,
        executor_cores=1,
        executor_memory="2G"

    )

    predict_task = SparkSubmitOperator(
        task_id='predict_task',
        conn_id='spark_default',
        application=f'{base_dir}/inference.py',
        application_args=['--test-in','tARTar78_test_out','--pred-out', 'tARTar78_hw6_prediction','--sklearn-model-in', f'{base_dir}/6.joblib'],
        env_vars={
            'PYSPARK_PYTHON': '/opt/conda/envs/dsenv/bin/python'
        },
        spark_binary=spark_binary1,
	num_executors = 4,
        executor_cores=1,
        executor_memory="2G"
    )

    feature_eng_train_task >> download_train_task >> train_task >> model_sensor >> feature_eng_test_task >> predict_task

