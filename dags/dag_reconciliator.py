"""
Reconciliator DAG — runs every 30 minutes.

For each pipeline × completed window it:
  1. Counts rows in source (Postgres) and target (BigQuery).
  2. Creates a pipeline_window_state record if none exists (PENDING → CHECKING).
  3. If counts match → marks RESOLVED.
  4. If counts differ → marks DIVERGENT and, if attempts < MAX_BACKFILL_ATTEMPTS,
     triggers the pipeline DAG in backfill mode and marks BACKFILLING.
  5. If attempts == MAX_BACKFILL_ATTEMPTS → marks FAILED (human escalation needed).
  6. Skips windows already in BACKFILLING (anti-thrash protection).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.api.common.trigger_dag import trigger_dag
from google.cloud.bigquery import Client

# Import pipeline registry from factory — single source of truth.
from dag_factory import PIPELINE_CONFIGS, WINDOW_HOURS

log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
RECONCILIATOR_CONN_ID = "reconciliator_db"  # Postgres connection for pipeline_window_state
MAX_BACKFILL_ATTEMPTS = 3
# How many completed windows to look back per reconciliation run.
LOOKBACK_WINDOWS = 8  # covers last 24 h (8 × 3 h)


# ── SQL statements ────────────────────────────────────────────────────────────

_UPSERT_CHECKING = """
    INSERT INTO pipeline_window_state
        (pipeline_id, window_start, window_end, status, checked_at, attempts)
    VALUES
        (%(pipeline_id)s, %(window_start)s, %(window_end)s, 'CHECKING', NOW(), 0)
    ON CONFLICT (pipeline_id, window_start)
    DO UPDATE SET
        status     = CASE
                         WHEN pipeline_window_state.status IN ('RESOLVED', 'BACKFILLING')
                         THEN pipeline_window_state.status   -- never overwrite terminal/active
                         ELSE 'CHECKING'
                     END,
        checked_at = NOW()
    RETURNING id, status, attempts
"""

_UPDATE_RESOLVED = """
    UPDATE pipeline_window_state
    SET status       = 'RESOLVED',
        source_count = %(source_count)s,
        target_count = %(target_count)s,
        resolved_at  = NOW()
    WHERE pipeline_id   = %(pipeline_id)s
      AND window_start  = %(window_start)s
"""

_UPDATE_DIVERGENT = """
    UPDATE pipeline_window_state
    SET status       = 'DIVERGENT',
        source_count = %(source_count)s,
        target_count = %(target_count)s,
        checked_at   = NOW()
    WHERE pipeline_id   = %(pipeline_id)s
      AND window_start  = %(window_start)s
"""

_UPDATE_BACKFILLING = """
    UPDATE pipeline_window_state
    SET status          = 'BACKFILLING',
        attempts        = attempts + 1,
        backfill_run_id = %(run_id)s
    WHERE pipeline_id   = %(pipeline_id)s
      AND window_start  = %(window_start)s
"""

_UPDATE_FAILED = """
    UPDATE pipeline_window_state
    SET status = 'FAILED'
    WHERE pipeline_id  = %(pipeline_id)s
      AND window_start = %(window_start)s
"""

_SELECT_STATE = """
    SELECT status, attempts
    FROM pipeline_window_state
    WHERE pipeline_id  = %(pipeline_id)s
      AND window_start = %(window_start)s
"""


# ── Core reconciliation logic ─────────────────────────────────────────────────

def reconcile_all(**context: Any) -> None:
    """Main task: iterate every pipeline × window and reconcile."""
    state_hook = PostgresHook(postgres_conn_id=RECONCILIATOR_CONN_ID)
    now = datetime.utcnow()

    for cfg in PIPELINE_CONFIGS:
        windows = _get_lookback_windows(now, WINDOW_HOURS, LOOKBACK_WINDOWS)
        for window_start, window_end in windows:
            _reconcile_window(state_hook, cfg, window_start, window_end)


def _reconcile_window(
    state_hook: PostgresHook,
    cfg: dict[str, Any],
    window_start: datetime,
    window_end: datetime,
) -> None:
    pipeline_id = cfg["pipeline_id"]
    log.info("[%s] Reconciling window [%s, %s)", pipeline_id, window_start, window_end)

    # Check if currently BACKFILLING → skip (anti-thrash).
    current = _get_current_state(state_hook, pipeline_id, window_start)
    if current and current["status"] == "BACKFILLING":
        log.info("[%s] Window is BACKFILLING — skipping.", pipeline_id)
        return
    if current and current["status"] == "RESOLVED":
        log.info("[%s] Window already RESOLVED — skipping.", pipeline_id)
        return

    # Upsert to CHECKING.
    row = state_hook.get_first(
        _UPSERT_CHECKING,
        parameters={"pipeline_id": pipeline_id, "window_start": window_start, "window_end": window_end},
    )
    # row = (id, status, attempts) — status may have been preserved if RESOLVED/BACKFILLING.
    if row and row[1] in ("RESOLVED", "BACKFILLING"):
        return

    # Count source.
    source_count = _count_postgres(cfg, window_start, window_end)
    # Count target.
    target_count = _count_bq(cfg, window_start, window_end)

    log.info("[%s] source=%d target=%d", pipeline_id, source_count, target_count)

    if source_count == target_count:
        state_hook.run(
            _UPDATE_RESOLVED,
            parameters={"pipeline_id": pipeline_id, "window_start": window_start,
                        "source_count": source_count, "target_count": target_count},
        )
        log.info("[%s] RESOLVED.", pipeline_id)
        return

    # Counts diverge.
    state_hook.run(
        _UPDATE_DIVERGENT,
        parameters={"pipeline_id": pipeline_id, "window_start": window_start,
                    "source_count": source_count, "target_count": target_count},
    )

    attempts = (row[2] if row else 0) or 0
    if attempts >= MAX_BACKFILL_ATTEMPTS:
        state_hook.run(
            _UPDATE_FAILED,
            parameters={"pipeline_id": pipeline_id, "window_start": window_start},
        )
        log.error(
            "[%s] FAILED after %d attempts. Window [%s, %s) needs manual intervention.",
            pipeline_id, attempts, window_start, window_end,
        )
        return

    # Trigger backfill DAG run.
    run_id = _trigger_backfill(pipeline_id, window_start, window_end)
    state_hook.run(
        _UPDATE_BACKFILLING,
        parameters={"pipeline_id": pipeline_id, "window_start": window_start, "run_id": run_id},
    )
    log.info("[%s] Triggered backfill run_id=%s (attempt %d/%d).", pipeline_id, run_id, attempts + 1, MAX_BACKFILL_ATTEMPTS)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_lookback_windows(
    now: datetime, window_hours: int, n: int
) -> list[tuple[datetime, datetime]]:
    """Return the last `n` completed tumbling windows before `now`."""
    slot = (now.hour // window_hours) * window_hours
    latest_end = now.replace(hour=slot, minute=0, second=0, microsecond=0)
    if latest_end > now:
        latest_end -= timedelta(hours=window_hours)

    windows = []
    end = latest_end
    for _ in range(n):
        start = end - timedelta(hours=window_hours)
        windows.append((start, end))
        end = start
    return windows


def _get_current_state(
    hook: PostgresHook, pipeline_id: str, window_start: datetime
) -> dict[str, Any] | None:
    row = hook.get_first(_SELECT_STATE, parameters={"pipeline_id": pipeline_id, "window_start": window_start})
    if row is None:
        return None
    return {"status": row[0], "attempts": row[1]}


def _count_postgres(cfg: dict[str, Any], start: datetime, end: datetime) -> int:
    hook = PostgresHook(postgres_conn_id=cfg["source_conn_id"])
    sql = f"""
        SELECT COUNT(*)
        FROM {cfg['source_schema']}.{cfg['source_table']}
        WHERE {cfg['ts_column']} >= %(start)s
          AND {cfg['ts_column']} <  %(end)s
    """
    result = hook.get_first(sql, parameters={"start": start, "end": end})
    return int(result[0])


def _count_bq(cfg: dict[str, Any], start: datetime, end: datetime) -> int:
    bq_hook = BigQueryHook(gcp_conn_id=cfg["gcp_conn_id"], use_legacy_sql=False)
    client: Client = bq_hook.get_client(project_id=cfg["project_id"])
    full_table = f"{cfg['project_id']}.{cfg['dataset']}.{cfg['bq_table']}"
    query = f"""
        SELECT COUNT(*)
        FROM `{full_table}`
        WHERE `{cfg['ts_column']}` >= TIMESTAMP('{start.isoformat()}')
          AND `{cfg['ts_column']}` <  TIMESTAMP('{end.isoformat()}')
    """
    result = list(client.query(query).result())
    return int(result[0][0])


def _trigger_backfill(pipeline_id: str, window_start: datetime, window_end: datetime) -> str:
    """Trigger a DAG run in backfill mode and return the run_id."""
    conf = {
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
    }
    run_id = f"backfill__{pipeline_id}__{window_start.strftime('%Y%m%dT%H%M%S')}"
    trigger_dag(
        dag_id=pipeline_id,
        run_id=run_id,
        conf=conf,
        replace_microseconds=False,
    )
    return run_id


# ── DAG definition ────────────────────────────────────────────────────────────

_DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=25),
}

with DAG(
    dag_id="dag_reconciliator",
    default_args=_DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/30 * * * *",  # every 30 minutes
    catchup=False,
    max_active_runs=1,  # never overlap reconciliation runs
    tags=["backfill-agent", "reconciliator"],
    doc_md=__doc__,
) as dag:
    PythonOperator(
        task_id="reconcile_all_pipelines",
        python_callable=reconcile_all,
        provide_context=True,
    )
