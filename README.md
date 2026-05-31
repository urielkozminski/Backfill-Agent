# Backfill Agent

Automatic reconciliation system for batch pipelines built on Apache Airflow.  
Detects gaps between a PostgreSQL source and BigQuery target, then triggers idempotent backfills to close them — no manual intervention required.

---

## How it works

```
Every 30 min
     │
     ▼
close_backfilling_windows          ← checks in-flight backfills via Airflow DagRun ORM
     │
     ▼
reconcile_all_pipelines            ← counts source vs target for last 24 h of windows
     │
     ├─ counts match   → RESOLVED
     └─ counts differ  → DIVERGENT → trigger pipeline DAG in backfill mode → BACKFILLING
                                     (max 3 attempts, then FAILED for human escalation)
```

Window model: fixed 3-hour tumbling windows aligned to UTC midnight  
(`00:00–03:00`, `03:00–06:00`, … `21:00–00:00`).

Idempotence: each backfill run issues a `DELETE` scoped to the window range  
followed by a fresh `INSERT` — re-running the same window always produces the same result.

---

## Repository structure

```
dags/
  dag_factory.py          # generates the 5 pipeline DAGs from PIPELINE_CONFIGS
  dag_reconciliator.py    # reconciliation DAG (runs every 30 min)
  utils/
    window.py             # tumbling window resolver (normal + backfill mode)
    transfer.py           # Postgres → BigQuery transfer with idempotence
setup/
  setup_postgres.sql      # DDL for pipeline_window_state (run once)
  setup_bq.py             # creates the 5 partitioned BigQuery tables (run once)
requirements.txt
```

---

## Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.10+ |
| Apache Airflow | 2.7+ |
| apache-airflow-providers-google | 10.0+ |
| apache-airflow-providers-postgres | 5.0+ |
| google-cloud-bigquery | 3.13+ |

Install Python dependencies:

```bash
pip install -r requirements.txt
```

---

## Setup — step by step

### 1. Configure Airflow connections

Create the following connections in Airflow (Admin → Connections):

| Conn Id | Type | Purpose |
|---|---|---|
| `source_db` | Postgres | Source database (all 5 pipelines read from here) |
| `reconciliator_db` | Postgres | Database that holds `pipeline_window_state` |
| `google_cloud_default` | Google Cloud | BigQuery authentication (Service Account JSON keyfile) |

For `google_cloud_default`:
- Connection type: `Google Cloud`
- Keyfile JSON: paste the full contents of your service account JSON

### 2. Create the state table in Postgres

Run against the database pointed to by `reconciliator_db`:

```bash
psql -h <host> -U <user> -d <database> -f setup/setup_postgres.sql
```

This creates `pipeline_window_state` with all required indexes.

### 3. Create the target tables in BigQuery

```bash
python setup/setup_bq.py --project <your-gcp-project> --dataset <your-dataset>
```

Use `--dry-run` to preview what would be created without executing:

```bash
python setup/setup_bq.py --project my-project --dataset dwh --dry-run
```

Authentication uses `GOOGLE_APPLICATION_CREDENTIALS` or `gcloud auth application-default login`.

### 4. Update pipeline configuration

Edit `dags/dag_factory.py` and set the correct values in `PIPELINE_CONFIGS`:

```python
# Replace these placeholders in every pipeline entry:
"gcp_conn_id": "google_cloud_default",   # Airflow connection id
"project_id":  "my-gcp-project",         # your GCP project id
"dataset":     "dwh",                    # your BigQuery dataset
```

### 5. Deploy DAGs to Airflow

Copy the `dags/` folder to your Airflow DAGs directory (or let your CI/CD pipeline do it):

```bash
cp -r dags/* $AIRFLOW_HOME/dags/
```

Airflow will detect and register 6 DAGs:
- `pipeline_ventas`
- `pipeline_inventario`
- `pipeline_clientes`
- `pipeline_logistica`
- `pipeline_facturacion`
- `dag_reconciliator`

### 6. Enable the DAGs

In the Airflow UI, unpause all 6 DAGs.  
`dag_reconciliator` will start on its next 30-minute tick.

---

## Window state reference

| Status | Meaning |
|---|---|
| `PENDING` | Window registered, not yet evaluated |
| `CHECKING` | Reconciliator is counting source vs target |
| `RESOLVED` | source_count == target_count ✓ |
| `DIVERGENT` | Counts differ — backfill about to be triggered |
| `BACKFILLING` | Pipeline DAG triggered, waiting for completion |
| `FAILED` | 3 backfill attempts exhausted — manual action required |

Query current status:

```sql
SELECT pipeline_id, window_start, window_end,
       source_count, target_count, status, attempts
FROM pipeline_window_state
WHERE status NOT IN ('RESOLVED')
ORDER BY window_start DESC;
```

---

## Triggering a manual backfill

To force a backfill for a specific window, trigger the pipeline DAG with `window_start` and `window_end` in the conf:

```bash
airflow dags trigger pipeline_ventas \
  --conf '{"window_start": "2024-01-15T06:00:00", "window_end": "2024-01-15T09:00:00"}'
```

Or via the Airflow UI: **Trigger DAG w/ config**.

---

## Adding a new pipeline

1. Add an entry to `PIPELINE_CONFIGS` in `dags/dag_factory.py`:

```python
{
    "pipeline_id":    "pipeline_nuevo",
    "source_conn_id": "source_db",
    "source_schema":  "schema_name",
    "source_table":   "table_name",
    "ts_column":      "created_at",
    "gcp_conn_id":    "google_cloud_default",
    "project_id":     "my-gcp-project",
    "dataset":        "dwh",
    "bq_table":       "table_name",
},
```

2. Add the table definition to `setup/setup_bq.py` and re-run it.

3. Deploy the updated `dag_factory.py` — Airflow will register the new DAG automatically.

---

## Escalation — handling FAILED windows

When a window reaches `FAILED`, the reconciliator stops retrying it automatically.

Steps to recover:

1. Investigate the root cause (check Airflow logs for the failed DAG runs).
2. Fix the underlying issue (data gap in source, BQ permissions, etc.).
3. Reset the window state to allow a fresh attempt:

```sql
UPDATE pipeline_window_state
SET status   = 'DIVERGENT',
    attempts = 0
WHERE pipeline_id  = 'pipeline_ventas'
  AND window_start = '2024-01-15 06:00:00';
```

4. The reconciliator will pick it up on its next run and trigger a new backfill.
