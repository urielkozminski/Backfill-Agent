"""
Creates the 5 target tables in BigQuery, each partitioned by its ts_column (DAY).

Usage:
    python setup/setup_bq.py --project my-gcp-project --dataset dwh

Authentication: uses Application Default Credentials or GOOGLE_APPLICATION_CREDENTIALS env var.
Run this once before the first pipeline execution.
"""

from __future__ import annotations

import argparse
import sys

from google.cloud import bigquery
from google.cloud.bigquery import Client, DatasetReference, SchemaField, Table, TimePartitioning, TimePartitioningType
from google.api_core.exceptions import Conflict

# ── Table definitions ─────────────────────────────────────────────────────────
# Each entry: (table_id, partition_field, schema_fields)
# Schema here is intentionally minimal/illustrative — adjust column types to
# match your actual Postgres source schema.

TABLE_DEFINITIONS: list[tuple[str, str, list[SchemaField]]] = [
    (
        "ventas",
        "created_at",
        [
            SchemaField("id", "INTEGER", mode="REQUIRED"),
            SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
            SchemaField("monto", "NUMERIC"),
            SchemaField("cliente_id", "INTEGER"),
        ],
    ),
    (
        "inventario",
        "updated_at",
        [
            SchemaField("id", "INTEGER", mode="REQUIRED"),
            SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
            SchemaField("producto_id", "INTEGER"),
            SchemaField("stock", "INTEGER"),
        ],
    ),
    (
        "clientes",
        "created_at",
        [
            SchemaField("id", "INTEGER", mode="REQUIRED"),
            SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
            SchemaField("nombre", "STRING"),
            SchemaField("email", "STRING"),
        ],
    ),
    (
        "envios",
        "despacho_at",
        [
            SchemaField("id", "INTEGER", mode="REQUIRED"),
            SchemaField("despacho_at", "TIMESTAMP", mode="REQUIRED"),
            SchemaField("origen", "STRING"),
            SchemaField("destino", "STRING"),
            SchemaField("estado", "STRING"),
        ],
    ),
    (
        "facturas",
        "emitida_at",
        [
            SchemaField("id", "INTEGER", mode="REQUIRED"),
            SchemaField("emitida_at", "TIMESTAMP", mode="REQUIRED"),
            SchemaField("monto_total", "NUMERIC"),
            SchemaField("cliente_id", "INTEGER"),
        ],
    ),
]


def create_tables(project: str, dataset: str, dry_run: bool = False) -> None:
    client: Client = bigquery.Client(project=project)
    dataset_ref = DatasetReference(project, dataset)

    for table_id, partition_field, schema in TABLE_DEFINITIONS:
        full_id = f"{project}.{dataset}.{table_id}"
        table = Table(full_id, schema=schema)
        table.time_partitioning = TimePartitioning(
            type_=TimePartitioningType.DAY,
            field=partition_field,
        )
        table.require_partition_filter = False  # set True in prod to control costs

        if dry_run:
            print(f"[DRY RUN] Would create: {full_id} partitioned by {partition_field}")
            continue

        try:
            client.create_table(table)
            print(f"Created: {full_id} (partitioned by {partition_field})")
        except Conflict:
            print(f"Already exists, skipping: {full_id}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Create BQ target tables for backfill-agent.")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--dataset", required=True, help="BigQuery dataset name")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be created without doing it")
    args = parser.parse_args()

    create_tables(project=args.project, dataset=args.dataset, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
