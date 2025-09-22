
from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="data_quality_checks",
    description="Schema, nulls, ranges, and enumerations for orders dataset",
    start_date=datetime(2025, 1, 1),
    schedule_interval="30 2 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["standard", "dq"],
) as dag:

    rowcount_ok = SqliteOperator(
        task_id="rowcount_ok",
        sqlite_conn_id="sqlite_default",
        sql="SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE RAISE(FAIL, 'No rows') END FROM raw_orders;",
    )

    no_null_pk = SqliteOperator(
        task_id="no_null_pk",
        sqlite_conn_id="sqlite_default",
        sql="SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE RAISE(FAIL, 'NULL PKs') END FROM raw_orders WHERE order_id IS NULL;",
    )

    amounts_non_negative = SqliteOperator(
        task_id="amounts_non_negative",
        sqlite_conn_id="sqlite_default",
        sql="SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE RAISE(FAIL, 'Negative amounts') END FROM raw_orders WHERE amount_usd < 0;",
    )

    status_enum = SqliteOperator(
        task_id="status_enum",
        sqlite_conn_id="sqlite_default",
        sql="SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE RAISE(FAIL, 'Bad status values') END FROM raw_orders WHERE UPPER(status) NOT IN ('PLACED','SHIPPED','DELIVERED','RETURNED');",
    )

    rowcount_ok >> no_null_pk >> amounts_non_negative >> status_enum
