"""
Core transfer logic: reads a time window from PostgreSQL and writes it to BigQuery.

Idempotence guarantee: BigQuery partition is overwritten via WRITE_TRUNCATE.
The BQ table must be partitioned by the pipeline's ts_column (DAY granularity).
For windows < 1 day or spanning midnight, we use a MERGE-free approach:
  1. Delete rows in the partition range with a DML DELETE.
  2. INSERT the fresh rows from the source.
This avoids needing to manage partition decorators manually while remaining
idempotent: re-running the same window always produces the same result.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.cloud.bigquery import Client, LoadJobConfig, QueryJobConfig, WriteDisposition
from google.cloud.bigquery.table import TableReference
import pandas as pd

log = logging.getLogger(__name__)


def transfer_window(
    *,
    source_conn_id: str,
    source_schema: str,
    source_table: str,
    ts_column: str,
    window_start: datetime,
    window_end: datetime,
    gcp_conn_id: str,
    project_id: str,
    dataset: str,
    bq_table: str,
) -> dict[str, Any]:
    """Extract a time window from Postgres and load it into a BigQuery table.

    Returns a dict with keys: source_count, rows_inserted, target_count.
    Raises ValueError if source_count != target_count after loading.
    """
    # ── 1. Count source rows ──────────────────────────────────────────────────
    pg_hook = PostgresHook(postgres_conn_id=source_conn_id)
    source_count = _count_source(pg_hook, source_schema, source_table, ts_column, window_start, window_end)
    log.info("Source count for window [%s, %s): %d", window_start, window_end, source_count)

    # ── 2. Extract from Postgres ──────────────────────────────────────────────
    rows_df = _extract_source(pg_hook, source_schema, source_table, ts_column, window_start, window_end)

    # ── 3. Load into BigQuery (idempotent: delete + insert within window) ─────
    bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id, use_legacy_sql=False)
    client: Client = bq_hook.get_client(project_id=project_id)
    full_table_id = f"{project_id}.{dataset}.{bq_table}"

    _delete_bq_window(client, full_table_id, ts_column, window_start, window_end)

    rows_inserted = 0
    if not rows_df.empty:
        rows_inserted = _insert_bq_rows(client, full_table_id, rows_df)

    # ── 4. Verify target count ────────────────────────────────────────────────
    target_count = _count_bq_window(client, full_table_id, ts_column, window_start, window_end)
    log.info("Target count after load: %d (inserted: %d)", target_count, rows_inserted)

    if source_count != target_count:
        raise ValueError(
            f"Count mismatch after transfer: source={source_count}, target={target_count} "
            f"for {full_table_id} window [{window_start}, {window_end})"
        )

    return {
        "source_count": source_count,
        "rows_inserted": rows_inserted,
        "target_count": target_count,
    }


# ── Postgres helpers ──────────────────────────────────────────────────────────

def _count_source(
    hook: PostgresHook,
    schema: str,
    table: str,
    ts_column: str,
    start: datetime,
    end: datetime,
) -> int:
    sql = f"""
        SELECT COUNT(*)
        FROM {schema}.{table}
        WHERE {ts_column} >= %(start)s
          AND {ts_column} <  %(end)s
    """
    result = hook.get_first(sql, parameters={"start": start, "end": end})
    return int(result[0])


def _extract_source(
    hook: PostgresHook,
    schema: str,
    table: str,
    ts_column: str,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    sql = f"""
        SELECT *
        FROM {schema}.{table}
        WHERE {ts_column} >= %(start)s
          AND {ts_column} <  %(end)s
    """
    conn = hook.get_conn()
    df = pd.read_sql(sql, conn, params={"start": start, "end": end})
    conn.close()
    return df


# ── BigQuery helpers ──────────────────────────────────────────────────────────

def _delete_bq_window(
    client: Client,
    full_table_id: str,
    ts_column: str,
    start: datetime,
    end: datetime,
) -> None:
    """Delete all rows in the window from BQ before re-inserting (idempotence)."""
    query = f"""
        DELETE FROM `{full_table_id}`
        WHERE DATE(`{ts_column}`) BETWEEN DATE('{start.date()}') AND DATE('{end.date()}')
          AND `{ts_column}` >= TIMESTAMP('{start.isoformat()}')
          AND `{ts_column}` <  TIMESTAMP('{end.isoformat()}')
    """
    job = client.query(query, job_config=QueryJobConfig())
    job.result()  # block until complete
    log.info("Deleted existing rows in BQ window [%s, %s)", start, end)


def _insert_bq_rows(client: Client, full_table_id: str, df: pd.DataFrame) -> int:
    """Load a DataFrame into BQ, appending to the existing table."""
    job_config = LoadJobConfig(write_disposition=WriteDisposition.WRITE_APPEND)
    job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    job.result()
    rows = job.output_rows
    log.info("Inserted %d rows into %s", rows, full_table_id)
    return rows


def _count_bq_window(
    client: Client,
    full_table_id: str,
    ts_column: str,
    start: datetime,
    end: datetime,
) -> int:
    query = f"""
        SELECT COUNT(*)
        FROM `{full_table_id}`
        WHERE `{ts_column}` >= TIMESTAMP('{start.isoformat()}')
          AND `{ts_column}` <  TIMESTAMP('{end.isoformat()}')
    """
    result = list(client.query(query).result())
    return int(result[0][0])
