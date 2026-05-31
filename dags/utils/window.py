"""
Utility for resolving the time window a DAG run should process.

Normal mode  : derives the window automatically from the DAG's logical date,
               aligned to fixed tumbling windows of `window_hours` hours.
Backfill mode: reads explicit window_start / window_end from dag_run.conf.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.utils.context import Context


def get_window(context: "Context", window_hours: int = 3) -> tuple[datetime, datetime]:
    """Return (window_start, window_end) for the current DAG run.

    In backfill mode dag_run.conf must contain ISO-8601 strings for both keys:
        {"window_start": "2024-01-15T06:00:00", "window_end": "2024-01-15T09:00:00"}

    In normal mode the window is the `window_hours`-wide slot that ended most
    recently before the DAG's logical_date, aligned to UTC midnight.
    """
    dag_run = context["dag_run"]
    conf: dict = dag_run.conf or {}

    if "window_start" in conf and "window_end" in conf:
        start = _parse_iso(conf["window_start"])
        end = _parse_iso(conf["window_end"])
        _validate_window(start, end, window_hours)
        return start, end

    logical_date: datetime = context["logical_date"]
    return _derive_window(logical_date, window_hours)


def _derive_window(reference: datetime, window_hours: int) -> tuple[datetime, datetime]:
    """Align `reference` to the previous completed tumbling window."""
    ref_utc = reference.astimezone(timezone.utc).replace(tzinfo=None)
    total_hours = ref_utc.hour
    slot = (total_hours // window_hours) * window_hours
    window_start = ref_utc.replace(hour=slot, minute=0, second=0, microsecond=0)

    # If we're exactly on a boundary, step back one full window so we always
    # process the *completed* window, never the one currently accumulating.
    if ref_utc == window_start:
        window_start -= timedelta(hours=window_hours)

    window_end = window_start + timedelta(hours=window_hours)
    return window_start, window_end


def _parse_iso(value: str) -> datetime:
    """Parse an ISO-8601 datetime string (with or without timezone)."""
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid ISO-8601 datetime in dag_run.conf: {value!r}") from exc
    # Normalize to naive UTC for consistency across the codebase.
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _validate_window(start: datetime, end: datetime, window_hours: int) -> None:
    expected_duration = timedelta(hours=window_hours)
    actual_duration = end - start
    if actual_duration != expected_duration:
        raise ValueError(
            f"Window duration mismatch: expected {window_hours}h, "
            f"got {actual_duration} ({start} → {end})"
        )
    if start.minute != 0 or start.second != 0 or start.microsecond != 0:
        raise ValueError(f"window_start is not aligned to a whole hour: {start}")
