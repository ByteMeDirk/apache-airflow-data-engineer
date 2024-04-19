import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from business_unit_operator_plugin import BusinessUnitOperator

default_args = {
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

DOC_MD = """
# Business Unit ETL DAG
## Description
This DAG demonstrates how to use the BusinessUnitOperator to process data for a given business unit.
"""


def process_data(ti, business_unit, **kwargs):
    """
    Process the data for the given business unit.

    Args:
        ti (TaskInstance): The task instance.
        business_unit (str): The business unit to process.

    Returns:
        None
    """
    s3_uris = ti.xcom_pull(task_ids="get_business_unit_uris")
    if s3_uris is None:
        logging.error("No data returned from 'get_business_unit_uris' task")
        return
    logging.info(f"Processing data for {business_unit}")
    logging.info("#======PROCESSING DATA======#")
    logging.info(f"Resource URIs: {s3_uris}")
    logging.info("#===========================#")


with DAG(
    dag_id="bu_etl_dag",
    description="Business Unit ETL DAG",
    default_args=default_args,
    start_date=days_ago(1),
    doc_md=DOC_MD,
    schedule_interval=None,
) as dag:
    get_business_unit_uris_task = BusinessUnitOperator(
        task_id="get_business_unit_uris", business_unit="analytics_squad_1", dag=dag
    )

    process_data_task = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
        op_kwargs={"business_unit": "analytics_squad_1"},
        provide_context=True,
        dag=dag,
    )

    get_business_unit_uris_task >> process_data_task
