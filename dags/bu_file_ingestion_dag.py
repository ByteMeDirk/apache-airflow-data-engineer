import logging
import os
import shutil
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.utils.dates import days_ago

AIRFLOW_DATASETS_LOCAL_DIR = "/opt/airflow/datasets"
AIRFLOW_OUTPUT_LOCAL_DIR = "/opt/airflow/output"
SQLITE_HOOK = SqliteHook(sqlite_conn_id="sqlite_conn")
CURRENT_DATE = datetime.today().strftime("%Y%m%d")
DEFAULT_ARGS = {
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}
DOC_MD = """
# Business Unit File Transfer DAG

This represents a complex DAG designed to consume BU files that land, to be ingested into
the SQLight database, and then exported to local output/export, before being compressed
and then moved to output/outbound_sftp, and finally archived in output/archive.
"""


def load_files_to_db(**kwargs):
    """
    Iterates the params.files and appends them to the sqlite database, the table name is
    the name of the file. The list of table names are then returned for the next task.
    """
    table_names = []
    for file in kwargs["params"]["ib_files"]:
        file_path = f"{AIRFLOW_DATASETS_LOCAL_DIR}/{file}"
        logging.info(f"Loading file {file_path} to database")
        df = pd.read_csv(file_path)
        table_name = file.split("/")[-1].replace(".csv", "")
        df.to_sql(table_name, SQLITE_HOOK.get_conn(), if_exists="append", index=False)
        table_names.append(table_name)

    return table_names


def update_audit_table(**kwargs):
    """
    Updates the audit table with the current date and the list of tables that were created.
    Create table if it does not exist.
    """
    table_names = kwargs["ti"].xcom_pull(task_ids="load_files_to_db")
    logging.info(f"Updating audit table with {table_names}")
    create_table_query = """
    CREATE TABLE IF NOT EXISTS ingest_audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        table_name TEXT
    )
    """
    SQLITE_HOOK.run(create_table_query)
    for table_name in table_names:
        insert_query = f"INSERT INTO ingest_audit (date, table_name) VALUES ('{CURRENT_DATE}', '{table_name}')"
        SQLITE_HOOK.run(insert_query)


def export_files_to_local(**kwargs):
    """
    Compares params.ob_tables with the audit tables for the current date,
    if the ob_table name is found in the audit table and has run today, then
    export the table to the output directory.
    A list of the output tables is returned for the next task.
    """
    ob_tables = kwargs["params"]["ob_tables"]
    audit_query = f"SELECT table_name FROM ingest_audit WHERE date='{CURRENT_DATE}'"
    audit_tables = SQLITE_HOOK.get_pandas_df(audit_query)["table_name"].tolist()
    export_tables = list(set(ob_tables) & set(audit_tables))
    logging.info(f"Exporting tables {export_tables}")
    for table in export_tables:
        export_query = f"SELECT * FROM {table}"
        df = SQLITE_HOOK.get_pandas_df(export_query)
        df.to_csv(f"{AIRFLOW_OUTPUT_LOCAL_DIR}/{kwargs['params']['output_dir']}/{table}_{CURRENT_DATE}.csv",
                  index=False)

    return export_tables


def compress_files(**kwargs):
    """
    Gathers all of the export files, compresses them, and deletes the original files.
    Compressed files are returned for the next task.
    """
    export_files = kwargs["ti"].xcom_pull(task_ids="export_files_to_local")
    file_list = []
    for file in export_files:
        file_path = f"{AIRFLOW_OUTPUT_LOCAL_DIR}/{kwargs['params']['output_dir']}/{file}_{CURRENT_DATE}.csv"
        logging.info(f"Compressing {file_path}")
        df = pd.read_csv(file_path)
        df.to_csv(f"{file_path}.gz", index=False)
        os.remove(file_path)
        file_list.append(f"{file}_{CURRENT_DATE}.csv.gz")

    return file_list


def sftp_files(**kwargs):
    compressed_files = kwargs["ti"].xcom_pull(task_ids="compress_files")
    create_table_query = """
    CREATE TABLE IF NOT EXISTS sftp_audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        files TEXT
    )
    """

    SQLITE_HOOK.run(create_table_query)
    for file in compressed_files:
        file_path = f"{AIRFLOW_OUTPUT_LOCAL_DIR}/{kwargs['params']['output_dir']}/{file}"
        if os.path.exists(file_path):
            if CURRENT_DATE not in file:
                logging.error(f"File {file} is not for today, moving to archive")
                shutil.move(file_path, f"{AIRFLOW_OUTPUT_LOCAL_DIR}/{kwargs['params']['archive_dir']}/{file}")
            else:
                if not os.path.exists(f"{AIRFLOW_OUTPUT_LOCAL_DIR}/{kwargs['params']['sftp_dir']}/{file}"):
                    logging.info(f"Copy {file} to SFTP")
                    shutil.copy(file_path, f"{AIRFLOW_OUTPUT_LOCAL_DIR}/{kwargs['params']['sftp_dir']}/{file}")
                    insert_query = f"INSERT INTO sftp_audit (date, files) VALUES ('{CURRENT_DATE}', '{file}')"
                    SQLITE_HOOK.run(insert_query)
        else:
            logging.error(f"File {file_path} does not exist")


def archive_files(**kwargs):
    """
    Archive any files left in the export directory.
    """
    export_files = os.listdir(f"{AIRFLOW_OUTPUT_LOCAL_DIR}/{kwargs['params']['output_dir']}")
    for file in export_files:
        shutil.move(f"{AIRFLOW_OUTPUT_LOCAL_DIR}/export/{file}",
                    f"{AIRFLOW_OUTPUT_LOCAL_DIR}/{kwargs['params']['archive_dir']}/{file}")

    # add ini.txt file to export directory
    with open(f"{AIRFLOW_OUTPUT_LOCAL_DIR}/{kwargs['params']['output_dir']}/ini.txt", "w") as f:
        f.write("This is an ini file, process is ready for another run.")


with DAG(
        dag_id="bu_file_ingestion_dag",
        description="Business Unit File Ingestion DAG",
        default_args=DEFAULT_ARGS,
        start_date=days_ago(1),
        schedule_interval=None,
        doc_md=DOC_MD,
        params={
            "ib_files": list(),
            "ob_tables": list(),
            "output_dir": "/export",
            "archive_dir": "/archive",
            "sftp_dir": "/sftp",
        }
) as dag:
    load_files = PythonOperator(
        task_id="load_files_to_db",
        python_callable=load_files_to_db,
        provide_context=True,
        dag=dag,
    )

    update_audit = PythonOperator(
        task_id="update_audit_table",
        python_callable=update_audit_table,
        provide_context=True,
        dag=dag,
    )

    export_files_to_local = PythonOperator(
        task_id="export_files_to_local",
        python_callable=export_files_to_local,
        provide_context=True,
        dag=dag,
    )

    compress_files = PythonOperator(
        task_id="compress_files",
        python_callable=compress_files,
        provide_context=True,
        dag=dag,
    )

    sftp_files = PythonOperator(
        task_id="sftp_files",
        python_callable=sftp_files,
        provide_context=True,
        dag=dag,
    )

    archive_files = PythonOperator(
        task_id="archive_files",
        python_callable=archive_files,
        provide_context=True,
        dag=dag,
    )

    load_files >> update_audit >> export_files_to_local >> compress_files >> sftp_files >> archive_files
