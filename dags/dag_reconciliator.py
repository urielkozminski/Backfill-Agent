"""
Reconciliator DAG — runs every 30 minutes.

Two tasks run sequentially each cycle:

  1. close_backfilling_windows
     For every window currently in BACKFILLING:
       - Queries Airflow's DagRun ORM using the stored backfill_run_id.
       - running  → skip (backfill still in progress).
       - success  → recount source vs target → RESOLVED or DIVERGENT.
       - failed   → DIVERGENT → trigger new backfill if attempts < MAX,
                    otherwise FAILED.

  2. reconcile_all_pipelines
     For every pipeline × completed window (last LOOKBACK_WINDOWS slots):
       - Skips RESOLVED and BACKFILLING windows.
       - Counts source (Postgres) and target (BigQuery).
       - Counts match   → RESOLVED.
       - Counts differ  → DIVERGENT → trigger backfill if attempts < MAX,
                          otherwise FAILED.

State machine:
  PENDING → CHECKING → RESOLVED
                     → DIVERGENT → BACKFILLING → RESOLVED
                                              → DIVERGENT (recount after backfill)
                                              → FAILED    (max attempts reached)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.models import DagRun
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.api.common.trigger_dag import trigger_dag
from airflow.utils.state import DagRunState
from google.cloud.bigquery import Client

from dag_factory import PIPELINE_CONFIGS, WINDOW_HOURS

log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
RECONCILIATOR_CONN_ID = "reconciliator_db"
MAX_BACKFILL_ATTEMPTS = 3
LOOKBACK_WINDOWS = 8  # last 24 h at 3 h/window

# ── SQL ───────────────────────────────────────────────────────────────────────

_UPSERT_CHECKING = """
    INSERT INTO pipeline_window_state
        (pipeline_id, window_start, window_end, status, checked_at, attempts)
    VALUES
        (%(pipeline_id)s, %(window_start)s, %(window_end)s, 'CHECKING', NOW(), 0)
    ON CONFLICT (pipeline_id, window_start)
    DO UPDATE SET
        status     = CASE
                         WHEN pipeline_window_state.status IN ('RESOLVED', 'BACKFILLING')
                         THEN pipeline_window_state.status
                         ELSE 'CHECKING'
                     END,
        checked_at = NOW()
    RETURNING id, status, attempts
"""

_SELECT_BACKFILLING = """
    SELECT pipeline_id, window_start, window_end, backfill_run_id, attempts
    FROM pipeline_window_state
    WHERE status = 'BACKFILLING'
    ORDER BY window_start
"""

_SELECT_STATE = """
    SELECT status, attempts
    FROM pipeline_window_state
    WHERE pipeline_id  = %(pipeline_id)s
      AND window_start = %(window_start)s
"""

_UPDATE_RESOLVED = """
    UPDATE pipeline_window_state
    SET status       = 'RESOLVED',
        source_count = %(source_count)s,
        target_count = %(target_count)s,
        resolved_at  = NOW(),
        checked_at   = NOW()
    WHERE pipeline_id  = %(pipeline_id)s
      AND window_start = %(window_start)s
"""

_UPDATE_DIVERGENT = """
    UPDATE pipeline_window_state
    SET status       = 'DIVERGENT',
        source_count = %(source_count)s,
        target_count = %(target_count)s,
        checked_at   = NOW()
    WHERE pipeline_id  = %(pipeline_id)s
      AND window_start = %(window_start)s
"""

_UPDATE_BACKFILLING = """
    UPDATE pipeline_window_state
    SET status          = 'BACKFILLING',
        attempts        = attempts + 1,
        backfill_run_id = %(run_id)s,
        checked_at      = NOW()
    WHERE pipeline_id  = %(pipeline_id)s
      AND window_start = %(window_start)s
"""

_UPDATE_FAILED = """
    UPDATE pipeline_window_state
    SET status     = 'FAILED',
        checked_at = NOW()
    WHERE pipeline_id  = %(pipeline_id)s
      AND window_start = %(window_start)s
"""


# ── Task 1: close BACKFILLING windows ─────────────────────────────────────────

def close_backfilling_windows(**context: Any) -> None:
    """Check every BACKFILLING window and close its cycle based on DagRun outcome."""
    state_hook = PostgresHook(postgres_conn_id=RECONCILIATOR_CONN_ID)
    rows = state_hook.get_records(_SELECT_BACKFILLING)

    if not rows:
        log.info("No windows in BACKFILLING — nothing to close.")
        return

    log.info("Found %d BACKFILLING window(s) to evaluate.", len(rows))

    for pipeline_id, window_start, window_end, backfill_run_id, attempts in rows:
        cfg = _get_pipeline_cfg(pipeline_id)
        if cfg is None:
            log.warning("Unknown pipeline_id '%s' in state table — skipping.", pipeline_id)
            continue

        _close_backfilling_window(
            state_hook=state_hook,
            cfg=cfg,
            pipeline_id=pipeline_id,
            window_start=window_start,
            window_end=window_end,
            backfill_run_id=backfill_run_id,
            attempts=attempts,
        )


def _close_backfilling_window(
    state_hook: PostgresHook,
    cfg: dict[str, Any],
    pipeline_id: str,
    window_start: datetime,
    window_end: datetime,
    backfill_run_id: str,
    attempts: int,
) -> None:
    dag_run_state = _get_dagrun_state(pipeline_id, backfill_run_id)

    if dag_run_state is None:
        log.warning(
            "[%s] DagRun '%s' not found in Airflow metadata — skipping.",
            pipeline_id, backfill_run_id,
        )
        return

    if dag_run_state == DagRunState.RUNNING:
        log.info("[%s] Backfill run '%s' still running — skipping.", pipeline_id, backfill_run_id)
        return

    log.info("[%s] Backfill run '%s' finished with state '%s'.", pipeline_id, backfill_run_id, dag_run_state)

    if dag_run_state == DagRunState.SUCCESS:
        # Recount to confirm — the pipeline DAG already validated this, but we
        # verify independently to guarantee the state table reflects reality.
        source_count = _count_postgres(cfg, window_start, window_end)
        target_count = _count_bq(cfg, window_start, window_end)
        log.info("[%s] Post-backfill recount: source=%d target=%d", pipeline_id, source_count, target_count)

        if source_count == target_count:
            state_hook.run(
                _UPDATE_RESOLVED,
                parameters={
                    "pipeline_id": pipeline_id,
                    "window_start": window_start,
                    "source_count": source_count,
                    "target_count": target_count,
                },
            )
            log.info("[%s] Window [%s, %s) → RESOLVED.", pipeline_id, window_start, window_end)
            return

        # DAG reported success but counts still differ — treat as DIVERGENT.
        log.error(
            "[%s] DAG succeeded but counts still differ (source=%d, target=%d). Marking DIVERGENT.",
            pipeline_id, source_count, target_count,
        )
        _handle_divergent(state_hook, cfg, pipeline_id, window_start, window_end, source_count, target_count, attempts)

    else:
        # DagRunState.FAILED or any other terminal state.
        log.warning("[%s] Backfill run failed (attempt %d/%d).", pipeline_id, attempts, MAX_BACKFILL_ATTEMPTS)
        # We don't have fresh counts here — keep whatever was last recorded.
        source_count = _last_source_count(state_hook, pipeline_id, window_start)
        target_count = _last_target_count(state_hook, pipeline_id, window_start)
        _handle_divergent(state_hook, cfg, pipeline_id, window_start, window_end, source_count, target_count, attempts)


# ── Task 2: detect gaps ────────────────────────────────────────────────────────

def reconcile_all_pipelines(**context: Any) -> None:
    """Detect gaps in all pipelines for the last LOOKBACK_WINDOWS completed windows."""
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

    # Fast path: avoid DB upsert for already-settled windows.
    current = _get_current_state(state_hook, pipeline_id, window_start)
    if current and current["status"] in ("RESOLVED", "BACKFILLING"):
        log.debug("[%s] Window [%s) is %s — skipping.", pipeline_id, window_start, current["status"])
        return

    # Upsert to CHECKING (idempotent — won't overwrite RESOLVED/BACKFILLING).
    row = state_hook.get_first(
        _UPSERT_CHECKING,
        parameters={"pipeline_id": pipeline_id, "window_start": window_start, "window_end": window_end},
    )
    if row and row[1] in ("RESOLVED", "BACKFILLING"):
        return

    source_count = _count_postgres(cfg, window_start, window_end)
    target_count = _count_bq(cfg, window_start, window_end)
    attempts = int(row[2]) if row else 0

    log.info("[%s] window_start=%s source=%d target=%d", pipeline_id, window_start, source_count, target_count)

    if source_count == target_count:
        state_hook.run(
            _UPDATE_RESOLVED,
            parameters={
                "pipeline_id": pipeline_id,
                "window_start": window_start,
                "source_count": source_count,
                "target_count": target_count,
            },
        )
        log.info("[%s] RESOLVED.", pipeline_id)
        return

    _handle_divergent(state_hook, cfg, pipeline_id, window_start, window_end, source_count, target_count, attempts)


# ── Shared divergence handler ─────────────────────────────────────────────────

def _handle_divergent(
    state_hook: PostgresHook,
    cfg: dict[str, Any],
    pipeline_id: str,
    window_start: datetime,
    window_end: datetime,
    source_count: int,
    target_count: int,
    attempts: int,
) -> None:
    """Write DIVERGENT and either trigger a new backfill or escalate to FAILED."""
    state_hook.run(
        _UPDATE_DIVERGENT,
        parameters={
            "pipeline_id": pipeline_id,
            "window_start": window_start,
            "source_count": source_count,
            "target_count": target_count,
        },
    )

    if attempts >= MAX_BACKFILL_ATTEMPTS:
        state_hook.run(_UPDATE_FAILED, parameters={"pipeline_id": pipeline_id, "window_start": window_start})
        log.error(
            "[%s] FAILED — %d/%d attempts exhausted for window [%s, %s). Manual intervention required.",
            pipeline_id, attempts, MAX_BACKFILL_ATTEMPTS, window_start, window_end,
        )
        return

    run_id = _trigger_backfill(pipeline_id, window_start, window_end)
    state_hook.run(
        _UPDATE_BACKFILLING,
        parameters={"pipeline_id": pipeline_id, "window_start": window_start, "run_id": run_id},
    )
    log.info(
        "[%s] Triggered backfill run_id=%s (attempt %d/%d).",
        pipeline_id, run_id, attempts + 1, MAX_BACKFILL_ATTEMPTS,
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_dagrun_state(dag_id: str, run_id: str) -> DagRunState | None:
    """Query Airflow's ORM for the terminal/current state of a DagRun."""
    runs: list[DagRun] = DagRun.find(dag_id=dag_id, run_id=run_id)
    if not runs:
        return None
    return runs[0].state


def _get_pipeline_cfg(pipeline_id: str) -> dict[str, Any] | None:
    return next((c for c in PIPELINE_CONFIGS if c["pipeline_id"] == pipeline_id), None)


def _get_current_state(
    hook: PostgresHook, pipeline_id: str, window_start: datetime
) -> dict[str, Any] | None:
    row = hook.get_first(_SELECT_STATE, parameters={"pipeline_id": pipeline_id, "window_start": window_start})
    return {"status": row[0], "attempts": row[1]} if row else None


def _last_source_count(hook: PostgresHook, pipeline_id: str, window_start: datetime) -> int:
    row = hook.get_first(
        "SELECT source_count FROM pipeline_window_state WHERE pipeline_id=%(p)s AND window_start=%(w)s",
        parameters={"p": pipeline_id, "w": window_start},
    )
    return int(row[0]) if row and row[0] is not None else 0


def _last_target_count(hook: PostgresHook, pipeline_id: str, window_start: datetime) -> int:
    row = hook.get_first(
        "SELECT target_count FROM pipeline_window_state WHERE pipeline_id=%(p)s AND window_start=%(w)s",
        parameters={"p": pipeline_id, "w": window_start},
    )
    return int(row[0]) if row and row[0] is not None else 0


def _get_lookback_windows(
    now: datetime, window_hours: int, n: int
) -> list[tuple[datetime, datetime]]:
    """Return the last `n` completed tumbling windows before `now`."""
    slot = (now.hour // window_hours) * window_hours
    latest_end = now.replace(hour=slot, minute=0, second=0, microsecond=0)
    if latest_end > now:
        latest_end -= timedelta(hours=window_hours)

    windows: list[tuple[datetime, datetime]] = []
    end = latest_end
    for _ in range(n):
        start = end - timedelta(hours=window_hours)
        windows.append((start, end))
        end = start
    return windows


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
        WHERE DATE(`{cfg['ts_column']}`) BETWEEN DATE('{start.date()}') AND DATE('{end.date()}')
          AND `{cfg['ts_column']}` >= TIMESTAMP('{start.isoformat()}')
          AND `{cfg['ts_column']}` <  TIMESTAMP('{end.isoformat()}')
    """
    result = list(client.query(query).result())
    return int(result[0][0])


def _trigger_backfill(pipeline_id: str, window_start: datetime, window_end: datetime) -> str:
    run_id = f"backfill__{pipeline_id}__{window_start.strftime('%Y%m%dT%H%M%S')}"
    trigger_dag(
        dag_id=pipeline_id,
        run_id=run_id,
        conf={
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
        },
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
    schedule_interval="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["backfill-agent", "reconciliator"],
    doc_md=__doc__,
) as dag:

    t_close = PythonOperator(
        task_id="close_backfilling_windows",
        python_callable=close_backfilling_windows,
        provide_context=True,
    )

    t_reconcile = PythonOperator(
        task_id="reconcile_all_pipelines",
        python_callable=reconcile_all_pipelines,
        provide_context=True,
    )

    # Close first so reconcile_all never re-triggers a window
    # that's about to be marked RESOLVED.
    t_close >> t_reconcile
