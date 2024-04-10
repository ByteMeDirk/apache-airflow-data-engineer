import logging
from datetime import timedelta

import pandas as pd
from airflow import DAG
from airflow import Dataset
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

DOC_MD = """
# Simple Producer DAG

A simple producer DAG that reads data from a CSV file, cleans the data, and saves the data to a CSV file.
"""

DATA_FILE_PATH = "/opt/airflow/datasets/purchase_data.csv"
OUTPUT_FILE_PATH = "/opt/airflow/datasets/output_purchase_data.csv"
PURCHASE_DATASET = Dataset(OUTPUT_FILE_PATH)

DEFAULT_ARGS = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def read_data() -> str:
    """
    Read the data from the CSV file.

    Returns:
        str: The data as json.
    """
    data_frame = pd.read_csv(DATA_FILE_PATH)
    logging.info(data_frame.head())
    return data_frame.to_json()


def clean_data(ti) -> str:
    """
    Clean the data.

    Args:
        ti (TaskInstance): The task instance.

    Returns:
        str: The data as json.
    """
    json_data = ti.xcom_pull(task_ids="read_data")
    data_frame = pd.read_json(json_data)
    data_frame = data_frame.dropna()
    return data_frame.to_json()


def save_data(ti) -> str:
    """
    Save the data to a CSV file.

    Args:
        ti (TaskInstance): The task instance.

    Returns:
        str: The data as json.
    """
    json_data = ti.xcom_pull(task_ids="clean_data")
    data_frame = pd.read_json(json_data)
    data_frame.to_csv(OUTPUT_FILE_PATH, index=False, mode="w", header=True)
    return data_frame.to_json()


with DAG(
    dag_id="simple_producer_purchase_dag",
    description="Simple Producer DAG for Purchase Data",
    start_date=days_ago(1),
    schedule_interval="@once",
    default_args=DEFAULT_ARGS,
    doc_md=DOC_MD,
    tags=["simple", "producer", "purchase"],
) as dag:
    read_data = PythonOperator(task_id="read_data", python_callable=read_data)

    clean_data = PythonOperator(task_id="clean_data", python_callable=clean_data)

    save_data = PythonOperator(
        task_id="save_data", python_callable=save_data, outlets=[PURCHASE_DATASET]
    )

    read_data >> clean_data >> save_data
