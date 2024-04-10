import logging
from datetime import timedelta

import pandas as pd
from airflow import DAG
from airflow import Dataset
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

DOC_MD = """
# Simple Consumer DAG

A simple consumer DAG that reads data from a CSV file, groups the data by city, and saves the data to a CSV file.
The schedule of this DAG is based on the schedule of the dataset.
"""

DATA_FILE_PATH = "/opt/airflow/datasets/output_customer_data.csv"
GROUPBY_OUTPUT_FILE_PATH = "/opt/airflow/datasets/groupby_customer_data.csv"

CUSTOMER_DATASET = Dataset(DATA_FILE_PATH)

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


def groupby_data(ti) -> str:
    """
    Groupby the data.

    Args:
        ti (TaskInstance): The task instance.

    Returns:
        str: The data as json.
    """
    json_data = ti.xcom_pull(task_ids="read_data")
    data_frame = pd.read_json(json_data)
    data_frame = data_frame.groupby("city").sum()
    return data_frame.to_json()


def save_data(ti) -> str:
    """
    Save the data to a CSV file.

    Args:
        ti (TaskInstance): The task instance.

    Returns:
        str: The data as json.
    """
    json_data = ti.xcom_pull(task_ids="groupby_data")
    data_frame = pd.read_json(json_data)
    data_frame.to_csv(GROUPBY_OUTPUT_FILE_PATH, index=False)
    return data_frame.to_json()


with DAG(
    dag_id="simple_consumer_customer_dag",
    description="Simple Consumer DAG for Customer Data",
    start_date=days_ago(1),
    schedule=[CUSTOMER_DATASET],
    default_args=DEFAULT_ARGS,
    doc_md=DOC_MD,
    tags=["simple", "consumer", "customer"],
) as dag:
    read_data = PythonOperator(task_id="read_data", python_callable=read_data)

    groupby_data = PythonOperator(task_id="groupby_data", python_callable=groupby_data)

    save_data = PythonOperator(task_id="save_data", python_callable=save_data)

    read_data >> groupby_data >> save_data
