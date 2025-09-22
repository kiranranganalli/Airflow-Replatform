
from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
import json, os

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="etl_orders_pipeline",
    description="Standardized ETL: ingest -> transform -> load with lineage + alerts",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["standard", "etl"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    @task(task_id="ingest", do_xcom_push=True)
    def ingest():
        os.system("python /opt/airflow/etl/ingest_orders.py")
        return {"dataset_in": "s3://landing/orders/*.csv", "dataset_out": "raw.orders"}

    @task(task_id="transform", do_xcom_push=True)
    def transform():
        os.system("python /opt/airflow/etl/transform_orders.py")
        return {"dataset_in": "raw.orders", "dataset_out": "staging.orders"}

    @task(task_id="load", do_xcom_push=True)
    def load():
        os.system("python /opt/airflow/etl/load_orders.py")
        return {"dataset_in": "staging.orders", "dataset_out": "mart.daily_revenue"}

    @task(task_id="emit_lineage")
    def emit_lineage(ing, tr, ld, **context):
        payload = {
            "run_id": context["run_id"],
            "ingest": ing,
            "transform": tr,
            "load": ld,
            "ts": datetime.utcnow().isoformat()
        }
        print("LINEAGE:", json.dumps(payload))

    ing = ingest()
    tr = transform()
    ld = load()
    emit = emit_lineage(ing, tr, ld)

    start >> ing >> tr >> ld >> emit >> end
