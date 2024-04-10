from time import sleep

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.utils.dates import days_ago

DOC_MD = """
# Simple SQL DAG

This DAG demonstrates how to use the Connection and Hook to interact with a SQLite database.
"""

default_args = {
    "catchup": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(
    dag_id="simple_sql_dag.py",
    description="Simple SQL DAG",
    start_date=days_ago(1),
    schedule_interval="*/1 * * * *",  # Every Minute
    default_args=default_args,
    doc_md=DOC_MD,
    tags=["simple", "sql"],
    catchup=False,
) as dag:

    def create_table():
        sqlite_hook = SqliteHook(sqlite_conn_id="sqlite_conn")
        create_table_query = """
        CREATE TABLE IF NOT EXISTS test_table (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value REAL NOT NULL
        );
        """
        sqlite_hook.run(create_table_query)

    def insert_data():
        # We define a mock sleep function to simulate a long-running task and SLA miss
        sleep(20)

        sqlite_hook = SqliteHook(sqlite_conn_id="sqlite_conn")
        insert_data_query = """
        INSERT INTO test_table (name, value)
        VALUES ('test_name', 123.45);
        """
        sqlite_hook.run(insert_data_query)

    def read_data():
        sqlite_hook = SqliteHook(sqlite_conn_id="sqlite_conn")
        select_data_query = """
        SELECT * FROM test_table;
        """
        records = sqlite_hook.get_records(select_data_query)
        return {"records": records}

    create_table = PythonOperator(task_id="create_table", python_callable=create_table)

    insert_data = PythonOperator(task_id="insert_data", python_callable=insert_data)

    read_data = PythonOperator(
        task_id="read_data", python_callable=read_data, do_xcom_push=True
    )

    create_table >> insert_data >> read_data
