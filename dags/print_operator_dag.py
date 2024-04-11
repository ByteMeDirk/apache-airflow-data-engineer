from datetime import datetime

from airflow import DAG
from print_operator_plugin import PrintOperator

default_args = {
    "start_date": datetime(2022, 1, 1),
}

with DAG("print_dag", default_args=default_args, schedule_interval=None) as dag:
    print_hello = PrintOperator(
        task_id="print_hello",
        message="Hello, world!",
    )
