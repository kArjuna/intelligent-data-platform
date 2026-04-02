from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess


def run_command(command):
    result = subprocess.run(
        command,
        capture_output=True,
        text=True
    )

    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    if result.returncode != 0:
        raise Exception(f"Command failed with exit code {result.returncode}")


def ingest_products():
    run_command(["python", "/opt/airflow/pipelines/ingestion/ingest_products.py"])


def ingest_customers():
    run_command(["python", "/opt/airflow/pipelines/ingestion/ingest_customers.py"])


def ingest_orders_enriched():
    run_command(["python", "/opt/airflow/pipelines/ingestion/ingest_orders_enriched.py"])


def run_sales_pipelines():
    run_command(["python", "/opt/airflow/pipelines/transformations/sales_pipelines.py"])


def run_customer_metrics():
    run_command(["python", "/opt/airflow/pipelines/transformations/customer_metrics.py"])


def run_product_metrics():
    run_command(["python", "/opt/airflow/pipelines/transformations/product_metrics.py"])


def run_revenue_by_category():
    run_command(["python", "/opt/airflow/pipelines/transformations/revenue_by_category.py"])


default_args = {
    "owner": "mallikarjuna_dag_multi_step_etl",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}


with DAG(
    dag_id="multi_step_etl_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Multi-step ETL pipeline with ingestion and transformations"
) as dag:

    ingest_products_task = PythonOperator(
        task_id="ingest_products",
        python_callable=ingest_products
    )

    ingest_customers_task = PythonOperator(
        task_id="ingest_customers",
        python_callable=ingest_customers
    )

    ingest_orders_enriched_task = PythonOperator(
        task_id="ingest_orders_enriched",
        python_callable=ingest_orders_enriched
    )

    sales_pipeline_task = PythonOperator(
        task_id="sales_pipeline",
        python_callable=run_sales_pipelines
    )

    customer_metrics_task = PythonOperator(
        task_id="customer_metrics",
        python_callable=run_customer_metrics
    )

    product_metrics_task = PythonOperator(
        task_id="product_metrics",
        python_callable=run_product_metrics
    )

    revenue_by_category_task = PythonOperator(
        task_id="revenue_by_category",
        python_callable=run_revenue_by_category
    )

    [
        ingest_products_task,
        ingest_customers_task,
        ingest_orders_enriched_task
    ] >> sales_pipeline_task

    sales_pipeline_task >> [
        customer_metrics_task,
        product_metrics_task,
        revenue_by_category_task
    ]
