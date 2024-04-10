import logging
from time import sleep

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.dates import days_ago

DOC_MD = """
# Simple Branching DAG

This DAG demonstrates how to use the `BranchPythonOperator` to create a simple branching workflow.
A Task SLA is set to 5 seconds to demonstrate the SLA miss callback.
"""

default_args = {
    "catchup": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
    "sla": pendulum.duration(seconds=5),
}


def sla_missed(*args, **kwargs):
    """
    Log a warning when the SLA is missed.
    """
    logging.warning("#======WARNING======#")
    logging.warning("==   SLA missed!   ==")
    logging.warning("#===================#")


def decide_which_path(**kwargs) -> str:
    """
    This function decides which path to take in the DAG based on the input argument.
    """
    sleep(20)

    # Get the input argument from kwargs
    input_arg = kwargs["dag_run"].conf.get("input_arg")

    # Use the input argument in the conditional statement
    return "branch_a" if input_arg == "a" else "branch_b"


with DAG(
    dag_id="simple_sla_branch_dag",
    description="Simple Branching DAG",
    start_date=days_ago(1),
    schedule_interval="*/1 * * * *",
    default_args=default_args,
    catchup=False,
    doc_md=DOC_MD,
    sla_miss_callback=sla_missed,
    tags=["simple", "branching"],
    params={"input_arg": None},
) as dag:
    start = DummyOperator(task_id="start")

    branch = BranchPythonOperator(
        task_id="branch", python_callable=decide_which_path, provide_context=True
    )

    branch_a = DummyOperator(task_id="branch_a")
    branch_b = DummyOperator(task_id="branch_b")

    end = DummyOperator(task_id="end")

    start >> branch >> [branch_a, branch_b] >> end
