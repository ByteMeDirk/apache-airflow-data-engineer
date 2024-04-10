import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

DOC_MD = """
# Simple Variable DAG

This DAG demonstrates how to use the Airflow Variables to create a simple variable workflow.
"""


def get_path_variable(**kwargs) -> str:
    """
    This function gets the path variable from the Airflow Variables.

    Returns:
        str: The path variable.
    """
    path = Variable.get("path", default_var="path_b")
    return path


def decide_which_path(**kwargs) -> str:
    """
    This function decides which path to take in the DAG based on the input argument.
    """
    # Get the input argument from kwargs
    input_arg = kwargs["dag_run"].conf.get("input_arg")

    # Use the input argument in the conditional statement
    return "branch_a" if input_arg == "a" else "branch_b"


with DAG(
    dag_id="simple_variable_dag",
    description="Simple Variable DAG",
    start_date=pendulum.datetime(2021, 1, 1, tz="Europe/London"),
    schedule_interval="@once",
    default_args={},
    catchup=False,
    doc_md=DOC_MD,
    tags=["simple", "variable"],
) as dag:
    start = DummyOperator(task_id="start")

    path = BranchPythonOperator(
        task_id="path", python_callable=get_path_variable, provide_context=True
    )

    path_a = DummyOperator(task_id="path_a")
    path_b = DummyOperator(task_id="path_b")

    end = DummyOperator(task_id="end")

    start >> path >> [path_a, path_b] >> end
