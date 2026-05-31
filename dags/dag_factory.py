"""
Factory that generates one Airflow DAG per pipeline from PIPELINE_CONFIGS.

Each generated DAG:
  - Runs on a 3-hour schedule aligned to UTC midnight.
  - Has a single PythonOperator that calls transfer_window().
  - Pushes the transfer result dict to XCom under key "transfer_result".
  - Fails explicitly if source_count != target_count (transfer_window raises).
  - Supports backfill mode via dag_run.conf (see utils/window.py).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.context import Context

from utils.transfer import transfer_window
from utils.window import get_window

log = logging.getLogger(__name__)

# ── Central pipeline registry ─────────────────────────────────────────────────
# Modify these values to match your environment.
# gcp_conn_id  : name of the Airflow Google Cloud connection (Service Account JSON).
# project_id   : GCP project that owns the BigQuery dataset.
# dataset      : BigQuery dataset where target tables live.

PIPELINE_CONFIGS: list[dict[str, Any]] = [
    {
        "pipeline_id": "pipeline_ventas",
        "source_conn_id": "source_db",
        "source_schema": "public",
        "source_table": "ventas",
        "ts_column": "created_at",
        "gcp_conn_id": "google_cloud_default",
        "project_id": "my-gcp-project",
        "dataset": "dwh",
        "bq_table": "ventas",
    },
    {
        "pipeline_id": "pipeline_inventario",
        "source_conn_id": "source_db",
        "source_schema": "public",
        "source_table": "inventario",
        "ts_column": "updated_at",
        "gcp_conn_id": "google_cloud_default",
        "project_id": "my-gcp-project",
        "dataset": "dwh",
        "bq_table": "inventario",
    },
    {
        "pipeline_id": "pipeline_clientes",
        "source_conn_id": "source_db",
        "source_schema": "public",
        "source_table": "clientes",
        "ts_column": "created_at",
        "gcp_conn_id": "google_cloud_default",
        "project_id": "my-gcp-project",
        "dataset": "dwh",
        "bq_table": "clientes",
    },
    {
        "pipeline_id": "pipeline_logistica",
        "source_conn_id": "source_db",
        "source_schema": "operaciones",
        "source_table": "envios",
        "ts_column": "despacho_at",
        "gcp_conn_id": "google_cloud_default",
        "project_id": "my-gcp-project",
        "dataset": "dwh",
        "bq_table": "envios",
    },
    {
        "pipeline_id": "pipeline_facturacion",
        "source_conn_id": "source_db",
        "source_schema": "finanzas",
        "source_table": "facturas",
        "ts_column": "emitida_at",
        "gcp_conn_id": "google_cloud_default",
        "project_id": "my-gcp-project",
        "dataset": "dwh",
        "bq_table": "facturas",
    },
]

WINDOW_HOURS = 3
MAX_RETRIES = 2
TASK_TIMEOUT = timedelta(minutes=30)

_DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": MAX_RETRIES,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": TASK_TIMEOUT,
}


def _make_transfer_callable(config: dict[str, Any]):
    """Return a closure that captures pipeline config and is picklable by Airflow."""

    def _run_transfer(context: Context) -> dict[str, Any]:
        window_start, window_end = get_window(context, window_hours=WINDOW_HOURS)
        log.info(
            "[%s] Processing window [%s, %s)",
            config["pipeline_id"],
            window_start,
            window_end,
        )
        result = transfer_window(
            source_conn_id=config["source_conn_id"],
            source_schema=config["source_schema"],
            source_table=config["source_table"],
            ts_column=config["ts_column"],
            window_start=window_start,
            window_end=window_end,
            gcp_conn_id=config["gcp_conn_id"],
            project_id=config["project_id"],
            dataset=config["dataset"],
            bq_table=config["bq_table"],
        )
        log.info("[%s] Transfer result: %s", config["pipeline_id"], result)

        # Push to XCom so the reconciliator can inspect the result if needed.
        context["ti"].xcom_push(key="transfer_result", value=result)
        return result

    _run_transfer.__name__ = f"transfer_{config['pipeline_id']}"
    return _run_transfer


def _build_dag(config: dict[str, Any]) -> DAG:
    pipeline_id = config["pipeline_id"]
    dag = DAG(
        dag_id=pipeline_id,
        default_args=_DEFAULT_ARGS,
        start_date=datetime(2024, 1, 1),
        schedule_interval=f"0 */{WINDOW_HOURS} * * *",  # every 3 hours
        catchup=False,
        tags=["backfill-agent", "pipeline"],
        doc_md=f"Auto-generated DAG for pipeline `{pipeline_id}`.",
    )

    with dag:
        PythonOperator(
            task_id="transfer_window",
            python_callable=_make_transfer_callable(config),
            provide_context=True,
        )

    return dag


# Register all DAGs into the module globals so Airflow's DagBag picks them up.
for _cfg in PIPELINE_CONFIGS:
    globals()[_cfg["pipeline_id"]] = _build_dag(_cfg)
