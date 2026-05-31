-- State table for the reconciliator.
-- Run once against the Airflow metadata DB or a dedicated reconciliator DB.

CREATE TABLE IF NOT EXISTS pipeline_window_state (
    id              SERIAL PRIMARY KEY,
    pipeline_id     VARCHAR(100)  NOT NULL,
    window_start    TIMESTAMP     NOT NULL,
    window_end      TIMESTAMP     NOT NULL,
    source_count    INTEGER,
    target_count    INTEGER,
    status          VARCHAR(20)   NOT NULL CHECK (status IN (
                        'PENDING', 'CHECKING', 'DIVERGENT',
                        'BACKFILLING', 'RESOLVED', 'FAILED'
                    )),
    backfill_run_id VARCHAR(100),
    checked_at      TIMESTAMP,
    resolved_at     TIMESTAMP,
    attempts        INTEGER NOT NULL DEFAULT 0,
    UNIQUE (pipeline_id, window_start)
);

CREATE INDEX IF NOT EXISTS idx_pws_status
    ON pipeline_window_state (status)
    WHERE status NOT IN ('RESOLVED', 'FAILED');

CREATE INDEX IF NOT EXISTS idx_pws_pipeline_window
    ON pipeline_window_state (pipeline_id, window_start DESC);

COMMENT ON TABLE pipeline_window_state IS
    'Tracks reconciliation state per (pipeline, 3-hour window). '
    'Managed exclusively by dag_reconciliator.';
