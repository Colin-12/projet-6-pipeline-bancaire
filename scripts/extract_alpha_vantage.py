"""
extract_alpha_vantage.py
------------------------
Extrait les indicateurs fondamentaux (PER, EPS, capitalisation, dividende)
de BNP Paribas, Société Générale et Crédit Agricole depuis Alpha Vantage
et dépose le CSV dans GCS.

Note : le free tier Alpha Vantage limite à 25 appels/jour.
3 tickers × 1 appel = 3 appels — largement dans le quota.

Usage :
    python scripts/extract_alpha_vantage.py
    python scripts/extract_alpha_vantage.py --date 2025-05-15
"""

import argparse
import io
import os
import sys
import time
from datetime import date, datetime

import pandas as pd
import requests
from google.cloud import storage

# ── Constantes ────────────────────────────────────────────────────────────────
TICKERS     = ["BNP.PA", "GLE.PA", "ACA.PA"]
BUCKET_NAME = os.environ.get("GCP_BUCKET_NAME", "projet6-raw-colin")
API_KEY     = os.environ.get("ALPHA_VANTAGE_KEY", "")
SOURCE_NAME = "alpha_vantage"
BASE_URL    = "https://www.alphavantage.co/query"

# Colonnes qu'on veut récupérer depuis l'endpoint OVERVIEW
FIELDS_OF_INTEREST = {
    "Symbol":              "ticker",
    "Name":                "company_name",
    "MarketCapitalization":"market_cap",
    "PERatio":             "per_ratio",
    "EPS":                 "eps",
    "DividendYield":       "dividend_yield",
    "BookValue":           "book_value",
    "PriceToBookRatio":    "price_to_book",
    "ReturnOnEquityTTM":   "roe",
    "ProfitMargin":        "profit_margin",
    "FiscalYearEnd":       "fiscal_year_end",
    "LatestQuarter":       "latest_quarter",
}


# ── Extraction ────────────────────────────────────────────────────────────────
def extract_ticker(ticker: str) -> dict | None:
    """
    Récupère les fondamentaux via yfinance .info
    (fallback car Alpha Vantage ne supporte pas les tickers .PA)
    """
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info

        if not info or "symbol" not in info:
            print(f"  ⚠️  {ticker} — données indisponibles")
            return None

        row = {
            "ticker":          ticker,
            "company_name":    info.get("longName"),
            "market_cap":      info.get("marketCap"),
            "per_ratio":       info.get("trailingPE"),
            "eps":             info.get("trailingEps"),
            "dividend_yield":  info.get("dividendYield"),
            "book_value":      info.get("bookValue"),
            "price_to_book":   info.get("priceToBook"),
            "roe":             info.get("returnOnEquity"),
            "profit_margin":   info.get("profitMargins"),
            "fiscal_year_end": info.get("lastFiscalYearEnd"),
            "latest_quarter":  info.get("mostRecentQuarter"),
        }

        print(f"  ✅ {ticker} — PER={row.get('per_ratio')}, MarketCap={row.get('market_cap')}")
        return row

    except Exception as e:
        print(f"  ❌ {ticker} — erreur : {e}")
        return None


# ── Upload GCS ────────────────────────────────────────────────────────────────
def upload_to_gcs(df: pd.DataFrame, target_date: date) -> tuple[bool, str]:
    """
    Dépose le CSV dans GCS sous :
    gs://{BUCKET}/{source}/{YYYY-MM-DD}/fundamentals.csv
    """
    date_str = target_date.strftime("%Y-%m-%d")
    gcs_path = f"{SOURCE_NAME}/{date_str}/fundamentals.csv"

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
    parser = argparse.ArgumentParser(description="Extrait les fondamentaux depuis Alpha Vantage")
    parser.add_argument("--date", type=str, default=None,
                        help="Date cible au format YYYY-MM-DD (défaut : aujourd'hui)")
    args = parser.parse_args()

    target_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today()

    print(f"\n📊 Extraction Alpha Vantage — {target_date}")
    print(f"   Tickers : {TICKERS}")
    print(f"   Bucket  : {BUCKET_NAME}\n")

    start_time = time.time()
    records_extracted = 0
    errors = []
    rows = []

    for i, ticker in enumerate(TICKERS):
        print(f"→ {ticker}")
        row = extract_ticker(ticker)

        if row is not None:
            row["extraction_date"] = str(target_date)
            rows.append(row)
            records_extracted += 1
        else:
            errors.append(ticker)

        # Délai entre les appels — Alpha Vantage free tier : max 5 req/min
        if i < len(TICKERS) - 1:
            print(f"  ⏳ Attente 15s (rate limit Alpha Vantage)...")
            

    duration = round(time.time() - start_time, 2)

    # Upload GCS
    records_loaded = 0
    status = "failure"
    error_message = None

    if rows:
        df = pd.DataFrame(rows)
        success, _ = upload_to_gcs(df, target_date)

        if success:
            records_loaded = len(df)
            status = "success" if not errors else "warning"
            if errors:
                error_message = f"Tickers en erreur : {errors}"
        else:
            status = "failure"
            error_message = "Échec upload GCS"
    else:
        error_message = f"Aucune donnée extraite. Tickers en erreur : {errors}"
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

    if status == "failure":
        sys.exit(1)


if __name__ == "__main__":
    main()
