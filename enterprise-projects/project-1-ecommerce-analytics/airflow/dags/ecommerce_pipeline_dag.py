"""
Orchestrates the daily batch pipeline: bronze ingest -> silver transform
-> gold (RFM + revenue leakage) -> ML segmentation -> BI export.

This DAG does NOT touch the Kafka/Structured Streaming path - that's a
long-running job Airflow doesn't manage well (it's not a "runs and
finishes" task, it's a "runs forever" service). In production it would be
its own always-on Spark application, started once and monitored
separately, while Airflow owns the recurring batch side shown here.

Each task is a plain spark-submit BashOperator, deliberately - a
SparkSubmitOperator (from apache-airflow-providers-apache-spark) is the
more idiomatic choice on a real Airflow deployment with a configured
Spark connection, but BashOperator keeps this DAG runnable by anyone who
just has `spark-submit` on PATH, which is what this classroom setup has
via 00-environment-setup.

Drop this file into $AIRFLOW_HOME/dags/ to schedule it.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/enterprise-projects/project-1-ecommerce-analytics"
SPARK_SUBMIT = "spark-submit --master local[*]"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_customer_analytics_daily",
    description="Bronze -> Silver -> Gold -> ML -> BI export for the e-commerce analytics platform",
    default_args=default_args,
    schedule="0 2 * * *",  # 2 AM daily, after the previous day's data has fully landed
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["enterprise-project-1", "ecommerce", "analytics"],
) as dag:

    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=f"{SPARK_SUBMIT} {PROJECT_DIR}/bronze/01_ingest_to_bronze.py",
    )

    transform_silver = BashOperator(
        task_id="transform_silver",
        bash_command=f"{SPARK_SUBMIT} {PROJECT_DIR}/silver/02_bronze_to_silver.py",
    )

    build_gold_rfm = BashOperator(
        task_id="build_gold_rfm_and_rollups",
        bash_command=f"{SPARK_SUBMIT} {PROJECT_DIR}/gold/03_silver_to_gold_rfm.py",
    )

    detect_revenue_leakage = BashOperator(
        task_id="detect_revenue_leakage",
        bash_command=f"{SPARK_SUBMIT} {PROJECT_DIR}/gold/04_revenue_leakage.py",
    )

    run_customer_segmentation = BashOperator(
        task_id="run_kmeans_segmentation",
        bash_command=f"{SPARK_SUBMIT} {PROJECT_DIR}/ml/05_customer_segmentation_kmeans.py",
    )

    export_for_bi = BashOperator(
        task_id="export_gold_for_bi",
        bash_command=f"{SPARK_SUBMIT} {PROJECT_DIR}/exports/08_export_gold_for_bi.py",
    )

    # gold_rfm and revenue_leakage both only need silver, and don't depend
    # on each other - fan them out in parallel rather than chaining them.
    ingest_bronze >> transform_silver >> [build_gold_rfm, detect_revenue_leakage]
    build_gold_rfm >> run_customer_segmentation
    [run_customer_segmentation, detect_revenue_leakage] >> export_for_bi
