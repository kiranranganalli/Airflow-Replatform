
from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="daily_reporting_job",
    description="Builds daily revenue aggregates and exports to reporting table",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["standard", "reporting"],
) as dag:

    make_table = SqliteOperator(
        task_id="make_table",
        sqlite_conn_id="sqlite_default",
        sql="""CREATE TABLE IF NOT EXISTS daily_report AS
               SELECT date(order_ts) AS ds, SUM(amount_usd) AS revenue_usd, COUNT(*) AS orders
               FROM staging_orders GROUP BY 1"""
    )

    def notify_success(**_):
        print("Report ready")

    notify = PythonOperator(task_id="notify", python_callable=notify_success)

    make_table >> notify
