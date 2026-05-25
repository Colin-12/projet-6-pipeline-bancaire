"""
log_pipeline_run.py
--------------------
Script transverse de logging.
Chaque étape du pipeline appelle log_run() pour tracer son exécution
dans la table BigQuery `projet6_mart.pipeline_runs`.

La table est créée automatiquement si elle n'existe pas.
"""

import os
import uuid
from datetime import datetime, timezone

from google.cloud import bigquery

# ── Constantes ────────────────────────────────────────────────────────────────
PROJECT_ID   = os.environ.get("GCP_PROJECT_ID", "projet6-pipeline-bancaire")
DATASET_ID   = "projet6_mart"
TABLE_ID     = "pipeline_runs"
FULL_TABLE   = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# ── Schéma de la table ────────────────────────────────────────────────────────
TABLE_SCHEMA = [
    bigquery.SchemaField("run_id",            "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("run_timestamp",     "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("source_name",       "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("step",              "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("records_extracted", "INT64",     mode="NULLABLE"),
    bigquery.SchemaField("records_loaded",    "INT64",     mode="NULLABLE"),
    bigquery.SchemaField("duration_seconds",  "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("status",            "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("error_message",     "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("dbt_tests_passed",  "INT64",     mode="NULLABLE"),
    bigquery.SchemaField("dbt_tests_failed",  "INT64",     mode="NULLABLE"),
]


# ── Création automatique de la table ─────────────────────────────────────────
def ensure_table_exists(client: bigquery.Client) -> None:
    """
    Crée la table pipeline_runs si elle n'existe pas encore.
    Idempotent — ne fait rien si la table existe déjà.
    """
    try:
        client.get_table(FULL_TABLE)
    except Exception:
        # Table inexistante — on la crée
        table = bigquery.Table(FULL_TABLE, schema=TABLE_SCHEMA)
        table = client.create_table(table)
        print(f"  ✅ Table créée : {FULL_TABLE}")


# ── Fonction principale ───────────────────────────────────────────────────────
def log_run(
    source_name:        str,
    step:               str,
    records_extracted:  int   = 0,
    records_loaded:     int   = 0,
    duration_seconds:   float = 0.0,
    status:             str   = "success",
    error_message:      str   = None,
    dbt_tests_passed:   int   = None,
    dbt_tests_failed:   int   = None,
) -> None:
    """
    Insère une ligne de log dans pipeline_runs.

    Paramètres :
        source_name       : nom de la source ('yfinance', 'alpha_vantage', 'banque_france', 'dbt')
        step              : étape ('extract', 'load', 'dbt_run', 'dbt_test')
        records_extracted : nombre de lignes extraites depuis la source
        records_loaded    : nombre de lignes chargées dans BigQuery
        duration_seconds  : durée de l'étape en secondes
        status            : 'success', 'warning', 'failure'
        error_message     : message d'erreur si applicable
        dbt_tests_passed  : nombre de tests dbt passés (pour step='dbt_test')
        dbt_tests_failed  : nombre de tests dbt échoués (pour step='dbt_test')
    """
    try:
        client = bigquery.Client(project=PROJECT_ID)
        ensure_table_exists(client)

        row = {
            "run_id":            str(uuid.uuid4()),
            "run_timestamp":     datetime.now(timezone.utc).isoformat(),
            "source_name":       source_name,
            "step":              step,
            "records_extracted": records_extracted,
            "records_loaded":    records_loaded,
            "duration_seconds":  round(duration_seconds, 2),
            "status":            status,
            "error_message":     error_message,
            "dbt_tests_passed":  dbt_tests_passed,
            "dbt_tests_failed":  dbt_tests_failed,
        }

        errors = client.insert_rows_json(FULL_TABLE, [row])

        if errors:
            print(f"  ⚠️  Erreur insertion pipeline_runs : {errors}")
        else:
            status_icon = {"success": "✅", "warning": "⚠️", "failure": "❌"}.get(status, "📝")
            print(f"  {status_icon} Log pipeline_runs — {source_name}/{step} — {status}")

    except Exception as e:
        # Le logging ne doit jamais bloquer le pipeline
        # On affiche l'erreur mais on ne lève pas d'exception
        print(f"  ⚠️  Impossible d'écrire dans pipeline_runs : {e}")


# ── Test standalone ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("🧪 Test log_pipeline_run.py\n")

    log_run(
        source_name       = "test",
        step              = "extract",
        records_extracted = 42,
        records_loaded    = 42,
        duration_seconds  = 1.23,
        status            = "success",
        error_message     = None
    )

    log_run(
        source_name       = "test",
        step              = "dbt_test",
        records_extracted = 0,
        records_loaded    = 0,
        duration_seconds  = 5.67,
        status            = "warning",
        error_message     = "1 test dbt échoué",
        dbt_tests_passed  = 11,
        dbt_tests_failed  = 1
    )

    print("\n✅ Test terminé — vérifie la table pipeline_runs dans BigQuery")
