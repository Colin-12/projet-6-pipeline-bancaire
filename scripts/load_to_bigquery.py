"""
load_to_bigquery.py
--------------------
Charge les fichiers CSV du jour depuis GCS vers les tables raw BigQuery.

Stratégie : append pur + colonne _ingested_at
- On n'écrase jamais les données existantes
- Chaque ligne sait quand elle a été chargée
- Les doublons éventuels seront gérés en staging (dbt)

Usage :
    python scripts/load_to_bigquery.py
    python scripts/load_to_bigquery.py --date 2025-05-15
"""

import argparse
import io
import os
import sys
import time
from datetime import date, datetime, timezone

import pandas as pd
from google.cloud import bigquery, storage

# ── Constantes ────────────────────────────────────────────────────────────────
PROJECT_ID  = os.environ.get("GCP_PROJECT_ID", "projet6-pipeline-bancaire")
BUCKET_NAME = os.environ.get("GCP_BUCKET_NAME", "projet6-raw-colin")
DATASET_RAW = "projet6_raw"

# Mapping : source → fichier GCS → table BigQuery + schéma
LOAD_CONFIG = {
    "yfinance": {
        "gcs_file":  "yfinance/{date}/cours.csv",
        "table":     "raw_market_data",
        "schema": [
            bigquery.SchemaField("ticker",        "STRING",    mode="REQUIRED"),
            bigquery.SchemaField("date_cotation", "DATE",      mode="REQUIRED"),
            bigquery.SchemaField("open",          "FLOAT64",   mode="NULLABLE"),
            bigquery.SchemaField("high",          "FLOAT64",   mode="NULLABLE"),
            bigquery.SchemaField("low",           "FLOAT64",   mode="NULLABLE"),
            bigquery.SchemaField("close",         "FLOAT64",   mode="NULLABLE"),
            bigquery.SchemaField("adj_close",     "FLOAT64",   mode="NULLABLE"),
            bigquery.SchemaField("volume",        "INT64",     mode="NULLABLE"),
            bigquery.SchemaField("_ingested_at",  "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("_source_file",  "STRING",    mode="NULLABLE"),
        ]
    },
    "alpha_vantage": {
        "gcs_file":  "alpha_vantage/{date}/fundamentals.csv",
        "table":     "raw_financials",
        "schema": [
            bigquery.SchemaField("ticker",         "STRING",    mode="REQUIRED"),
            bigquery.SchemaField("company_name",   "STRING",    mode="NULLABLE"),
            bigquery.SchemaField("market_cap",     "FLOAT64",   mode="NULLABLE"),
            bigquery.SchemaField("per_ratio",      "FLOAT64",   mode="NULLABLE"),
            bigquery.SchemaField("eps",            "FLOAT64",   mode="NULLABLE"),
            bigquery.SchemaField("dividend_yield", "FLOAT64",   mode="NULLABLE"),
            bigquery.SchemaField("book_value",     "FLOAT64",   mode="NULLABLE"),
            bigquery.SchemaField("price_to_book",  "FLOAT64",   mode="NULLABLE"),
            bigquery.SchemaField("roe",            "FLOAT64",   mode="NULLABLE"),
            bigquery.SchemaField("profit_margin",  "FLOAT64",   mode="NULLABLE"),
            bigquery.SchemaField("fiscal_year_end","STRING",    mode="NULLABLE"),
            bigquery.SchemaField("latest_quarter", "STRING",    mode="NULLABLE"),
            bigquery.SchemaField("extraction_date","DATE",      mode="NULLABLE"),
            bigquery.SchemaField("_ingested_at",   "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("_source_file",   "STRING",    mode="NULLABLE"),
        ]
    },
    "banque_france": {
        "gcs_file":  "banque_france/{date}/taux.csv",
        "table":     "raw_macro",
        "schema": [
            bigquery.SchemaField("indicator_code",   "STRING",    mode="REQUIRED"),
            bigquery.SchemaField("series_id",        "STRING",    mode="NULLABLE"),
            bigquery.SchemaField("date_observation", "DATE",      mode="NULLABLE"),
            bigquery.SchemaField("value",            "FLOAT64",   mode="NULLABLE"),
            bigquery.SchemaField("_ingested_at",     "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("_source_file",     "STRING",    mode="NULLABLE"),
        ]
    },
}


# ── Helpers ───────────────────────────────────────────────────────────────────
def ensure_table_exists(client: bigquery.Client, table_id: str, schema: list) -> None:
    """Crée la table BigQuery si elle n'existe pas encore."""
    try:
        client.get_table(table_id)
    except Exception:
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
        print(f"  ✅ Table créée : {table_id}")


def read_csv_from_gcs(bucket_name: str, gcs_path: str) -> pd.DataFrame | None:
    """Lit un CSV depuis GCS et retourne un DataFrame."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob   = bucket.blob(gcs_path)

        if not blob.exists():
            print(f"  ⚠️  Fichier introuvable dans GCS : {gcs_path}")
            return None

        content = blob.download_as_text()
        df = pd.read_csv(io.StringIO(content))
        return df

    except Exception as e:
        print(f"  ❌ Erreur lecture GCS : {e}")
        return None


def load_dataframe_to_bq(
    client:     bigquery.Client,
    df:         pd.DataFrame,
    table_id:   str,
    schema:     list,
    source_file:str
) -> int:
    """
    Charge un DataFrame dans BigQuery en mode APPEND.
    Ajoute les colonnes _ingested_at et _source_file.
    Retourne le nombre de lignes chargées.
    """
    # Ajouter les colonnes de traçabilité
    df["_ingested_at"] = datetime.now(timezone.utc)
    df["_source_file"] = source_file

    # Convertir les colonnes DATE du schéma en objets date Python
    # (pyarrow gère les date objects natifs, pas les strings)
    date_cols = [f.name for f in schema if f.field_type == "DATE"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    
    # Convertir les colonnes STRING du schéma qui auraient été lues comme float
    string_cols = [f.name for f in schema if f.field_type == "STRING"]
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).replace("nan", None).replace("<NA>", None)
    
    # Remplacer les NaN par None (BigQuery attend NULL, pas NaN)
    df = df.where(pd.notnull(df), None)

    job_config = bigquery.LoadJobConfig(
        schema          = schema,
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND,
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Attendre la fin du job

    return len(df)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Charge les CSVs GCS vers BigQuery raw")
    parser.add_argument("--date", type=str, default=None,
                        help="Date cible au format YYYY-MM-DD (défaut : aujourd'hui)")
    args = parser.parse_args()

    target_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today()
    date_str    = target_date.strftime("%Y-%m-%d")

    print(f"\n📥 Chargement BigQuery — {date_str}")
    print(f"   Dataset : {PROJECT_ID}.{DATASET_RAW}\n")

    bq_client  = bigquery.Client(project=PROJECT_ID)
    start_time = time.time()

    total_loaded = 0
    errors       = []

    for source_name, config in LOAD_CONFIG.items():
        print(f"→ {source_name}")

        gcs_path = config["gcs_file"].format(date=date_str)
        table_id = f"{PROJECT_ID}.{DATASET_RAW}.{config['table']}"

        # S'assurer que la table existe
        ensure_table_exists(bq_client, table_id, config["schema"])

        # Lire le CSV depuis GCS
        df = read_csv_from_gcs(BUCKET_NAME, gcs_path)

        if df is None or df.empty:
            print(f"  ⏭️  {source_name} — pas de données à charger")
            errors.append(source_name)
            continue

        print(f"  📄 {len(df)} ligne(s) lues depuis GCS")

        # Charger dans BigQuery
        try:
            n_loaded = load_dataframe_to_bq(
                client      = bq_client,
                df          = df.copy(),
                table_id    = table_id,
                schema      = config["schema"],
                source_file = f"gs://{BUCKET_NAME}/{gcs_path}"
            )
            total_loaded += n_loaded
            print(f"  ✅ {n_loaded} ligne(s) chargées → {config['table']}")

        except Exception as e:
            print(f"  ❌ Erreur chargement {source_name} : {e}")
            errors.append(source_name)

    duration = round(time.time() - start_time, 2)
    status   = "success" if not errors else ("warning" if total_loaded > 0 else "failure")
    error_msg = f"Sources en erreur : {errors}" if errors else None

    # Log pipeline_runs
    try:
        from log_pipeline_run import log_run
        log_run(
            source_name       = "all_sources",
            step              = "load",
            records_extracted = total_loaded,
            records_loaded    = total_loaded,
            duration_seconds  = duration,
            status            = status,
            error_message     = error_msg
        )
    except Exception as e:
        print(f"  ⚠️  Impossible de logger : {e}")

    # Résumé
    print(f"\n{'✅' if status == 'success' else '⚠️' if status == 'warning' else '❌'} Résumé")
    print(f"   Lignes chargées : {total_loaded}")
    print(f"   Durée           : {duration}s")
    print(f"   Statut          : {status}")
    if error_msg:
        print(f"   Erreur          : {error_msg}")

    if status == "failure":
        sys.exit(1)


if __name__ == "__main__":
    main()
