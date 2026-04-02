# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import os

# def run_etl():
#     os.system("python scripts/etl_pipeline.py")

# default_args = {
#     'owner': 'arjuna',
#     'start_date': datetime(2024, 1, 1),
# }

# with DAG(
#     dag_id='etl_pipeline_dag',
#     default_args=default_args,
#     schedule_interval='@daily',
#     catchup=False
# ) as dag:

#     etl_task = PythonOperator(
#         task_id='run_etl',
#         python_callable=run_etl
#     )

#     etl_task

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess


def run_etl():
    result = subprocess.run(
        ["python", "/opt/airflow/scripts/etl_pipeline.py"],
        capture_output=True,
        text=True
    )

    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    if result.returncode != 0:
        raise Exception(f"ETL script failed with exit code {result.returncode}")


default_args = {
    "owner": "mallikarjuna",
    "start_date": datetime(2024, 1, 1)
}

with DAG(
    dag_id="etl_pipeline_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    etl_task = PythonOperator(
        task_id="run_etl",
        python_callable=run_etl
    )
    etl_task