import logging
from datetime import timedelta

import pandas as pd
from airflow import DAG
from airflow import Dataset
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

DOC_MD = """
# Simple Consumer DAG

A simple consumer DAG that reads data from a CSV file and joins to customer data by customer_id.
The schedule of this DAG is based on the schedule of the dataset.
"""

CUSTOMER_DATA_FILE_PATH = "/opt/airflow/datasets/output_customer_data.csv"
PURCHASER_DATA_FILE_PATH = "/opt/airflow/datasets/output_purchase_data.csv"
REPORT_OUTPUT_FILE_PATH = "/opt/airflow/datasets/report_data.csv"

CUSTOMER_DATASET = Dataset(CUSTOMER_DATA_FILE_PATH)
PURCHASER_DATASET = Dataset(PURCHASER_DATA_FILE_PATH)

DEFAULT_ARGS = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def read_customer_data() -> str:
    """
    Read the customer data from the CSV file.

    Returns:
        str: The customer data as json.
    """
    data_frame = pd.read_csv(CUSTOMER_DATA_FILE_PATH)
    logging.info(data_frame.head())
    return data_frame.to_json()


def read_purchaser_data() -> str:
    """
    Read the purchaser data from the CSV file.

    Returns:
        str: The purchaser data as json.
    """
    data_frame = pd.read_csv(PURCHASER_DATA_FILE_PATH)
    logging.info(data_frame.head())
    return data_frame.to_json()


def join_data(ti) -> str:
    """
    Join the data.

    Args:
        ti (TaskInstance): The task instance.

    Returns:
        str: The data as json.
    """
    customer_json_data = ti.xcom_pull(task_ids="read_customer_data")
    purchaser_json_data = ti.xcom_pull(task_ids="read_purchaser_data")

    customer_data_frame = pd.read_json(customer_json_data)
    purchaser_data_frame = pd.read_json(purchaser_json_data)

    data_frame = pd.merge(
        customer_data_frame, purchaser_data_frame, on="customer_id", how="inner"
    )
    return data_frame.to_json()


def save_data(ti) -> str:
    """
    Save the data to a CSV file.

    Args:
        ti (TaskInstance): The task instance.

    Returns:
        str: The data as json.
    """
    json_data = ti.xcom_pull(task_ids="join_data")
    data_frame = pd.read_json(json_data)
    data_frame.to_csv(REPORT_OUTPUT_FILE_PATH, index=False)
    return data_frame.to_json()



with DAG(
    dag_id="simple_consumer_purchase_dag",
    description="Simple Consumer DAG for Purchase Data",
    start_date=days_ago(1),
    schedule=[CUSTOMER_DATASET, PURCHASER_DATASET],
    default_args=DEFAULT_ARGS,
    doc_md=DOC_MD,
    tags=["simple", "consumer", "purchase"],
) as dag:
    read_customer_data = PythonOperator(
        task_id="read_customer_data", python_callable=read_customer_data
    )

    read_purchaser_data = PythonOperator(
        task_id="read_purchaser_data", python_callable=read_purchaser_data
    )

    join_data = PythonOperator(task_id="join_data", python_callable=join_data)

    save_data = PythonOperator(task_id="save_data", python_callable=save_data)

    read_customer_data >> join_data
    read_purchaser_data >> join_data
    join_data >> save_data