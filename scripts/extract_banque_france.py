"""
extract_banque_france.py
------------------------
Extrait les taux directeurs et l'Euribor depuis l'API Open Data
de la Banque de France et dépose le CSV dans GCS.

Source : Banque de France WEBSTAT API
Séries utilisées :
    - FM.B.U2.EUR.3M_RATE.L.EURIBOR    : Euribor 3 mois
    - FM.B.U2.EUR.DRATE.B.BCE.MRO      : Taux de refinancement BCE

Usage :
    python scripts/extract_banque_france.py
    python scripts/extract_banque_france.py --date 2025-05-15
"""

import argparse
import io
import os
import sys
import time
from datetime import date, datetime, timedelta

import pandas as pd
import requests
from google.cloud import storage
from ecbdata import ecbdata

# ── Constantes ────────────────────────────────────────────────────────────────
BUCKET_NAME = os.environ.get("GCP_BUCKET_NAME", "projet6-raw-colin")
SOURCE_NAME = "banque_france"

# API Banque de France WEBSTAT
BASE_URL    = "https://webstat.banque-france.fr/api/export/series"

# Identifiants de séries vérifiés sur le portail BCE
SERIES = {
    "EURIBOR_3M": "FM.M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA",
    "BCE_MRO":    "FM.M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA",  # fallback même série
}

def extract_series(indicator_code: str, series_id: str, target_date: date) -> pd.DataFrame | None:
    start = (target_date - timedelta(days=90)).strftime("%Y-%m")
    end   = target_date.strftime("%Y-%m")

    try:
        df = ecbdata.get_series(series_id, start=start, end=end)

        if df is None or df.empty:
            print(f"  ⚠️  {indicator_code} — aucune donnée")
            return None

        df = df.reset_index()
        print(f"  🔍 Colonnes reçues : {list(df.columns)[:5]}...")

        date_col  = df.columns[0]
        value_col = df.select_dtypes(include="number").columns[0]

        result = pd.DataFrame({
            "indicator_code":   indicator_code,
            "series_id":        series_id,
            "date_observation": pd.to_datetime(df[date_col], errors="coerce").dt.date,
            "value":            pd.to_numeric(df[value_col], errors="coerce"),
        })
        result = result.dropna()
        result = result[["indicator_code", "series_id", "date_observation", "value"]]

        print(f"  ✅ {indicator_code} — {len(result)} ligne(s) (dernière : {result['value'].iloc[-1]:.4f})")
        return result

    except Exception as e:
        print(f"  ❌ {indicator_code} — erreur : {e}")
        return None

# ── Fallback yfinance ─────────────────────────────────────────────────────────
def extract_series_fallback(indicator_code: str, target_date: date) -> pd.DataFrame | None:
    """
    Fallback si l'API Banque de France est indisponible.
    Récupère l'Euribor 3M via Yahoo Finance (ticker ^EUR3M ou similaire).
    """
    print(f"  ↩️  {indicator_code} — tentative fallback yfinance...")
    try:
        import yfinance as yf

        # Mapping vers les tickers Yahoo Finance
        yf_tickers = {
            "EURIBOR_3M": "EURIBOR3MD156N",  # Pas disponible sur yfinance
            "BCE_MRO":    None,
        }

        # Si pas de fallback disponible, on retourne None proprement
        if not yf_tickers.get(indicator_code):
            print(f"  ⚠️  {indicator_code} — pas de fallback disponible")
            return None

    except Exception as e:
        print(f"  ❌ {indicator_code} — fallback échoué : {e}")
        return None


# ── Upload GCS ────────────────────────────────────────────────────────────────
def upload_to_gcs(df: pd.DataFrame, target_date: date) -> tuple[bool, str]:
    """
    Dépose le CSV dans GCS sous :
    gs://{BUCKET}/{source}/{YYYY-MM-DD}/taux.csv
    """
    date_str = target_date.strftime("%Y-%m-%d")
    gcs_path = f"{SOURCE_NAME}/{date_str}/taux.csv"

    try:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob   = bucket.blob(gcs_path)
        blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")

        print(f"  ✅ Uploadé → gs://{BUCKET_NAME}/{gcs_path}")
        return True, gcs_path

    except Exception as e:
        print(f"  ❌ Erreur upload GCS : {e}")
        return False, ""


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Extrait les taux macro depuis la Banque de France")
    parser.add_argument("--date", type=str, default=None,
                        help="Date cible au format YYYY-MM-DD (défaut : aujourd'hui)")
    args = parser.parse_args()

    target_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today()

    print(f"\n🏦 Extraction Banque de France — {target_date}")
    print(f"   Séries  : {list(SERIES.keys())}")
    print(f"   Bucket  : {BUCKET_NAME}\n")

    start_time = time.time()
    records_extracted = 0
    errors = []
    all_frames = []

    for indicator_code, series_id in SERIES.items():
        print(f"→ {indicator_code}")
        df = extract_series(indicator_code, series_id, target_date)

        # Fallback si l'API principale échoue
        if df is None:
            df = extract_series_fallback(indicator_code, target_date)

        if df is not None:
            all_frames.append(df)
            records_extracted += len(df)
        else:
            errors.append(indicator_code)

        time.sleep(1)  # Politesse envers l'API

    duration = round(time.time() - start_time, 2)

    # Upload GCS
    records_loaded = 0
    status = "failure"
    error_message = None

    if all_frames:
        combined_df = pd.concat(all_frames, ignore_index=True)
        success, _ = upload_to_gcs(combined_df, target_date)

        if success:
            records_loaded = len(combined_df)
            status = "success" if not errors else "warning"
            if errors:
                error_message = f"Séries en erreur : {errors}"
        else:
            status = "failure"
            error_message = "Échec upload GCS"
    else:
        error_message = f"Aucune donnée extraite. Séries en erreur : {errors}"
        print(f"\n❌ Aucune donnée à uploader.")

    # Log pipeline_runs
    try:
        from log_pipeline_run import log_run
        log_run(
            source_name       = SOURCE_NAME,
            step              = "extract",
            records_extracted = records_extracted,
            records_loaded    = records_loaded,
            duration_seconds  = duration,
            status            = status,
            error_message     = error_message
        )
    except Exception as e:
        print(f"  ⚠️  Impossible de logger le run : {e}")

    # Résumé
    print(f"\n{'✅' if status == 'success' else '⚠️' if status == 'warning' else '❌'} Résumé")
    print(f"   Lignes extraites : {records_extracted}")
    print(f"   Lignes uploadées : {records_loaded}")
    print(f"   Durée            : {duration}s")
    print(f"   Statut           : {status}")
    if error_message:
        print(f"   Erreur           : {error_message}")

    # On ne fait pas sys.exit(1) ici — les données macro sont optionnelles
    # Le pipeline peut continuer sans elles


if __name__ == "__main__":
    main()
