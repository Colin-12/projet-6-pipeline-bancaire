"""
extract_yfinance.py
-------------------
Extrait les cours boursiers quotidiens de BNP Paribas, Société Générale
et Crédit Agricole depuis Yahoo Finance et dépose le CSV dans GCS.

Usage :
    python scripts/extract_yfinance.py
    python scripts/extract_yfinance.py --date 2025-05-15   # rejouer une date passée
"""

import argparse
import io
import os
import sys
import time
from datetime import date, datetime, timedelta

import pandas as pd
import yfinance as yf
from google.cloud import storage

# ── Constantes ────────────────────────────────────────────────────────────────
TICKERS     = ["BNP.PA", "GLE.PA", "ACA.PA"]
BUCKET_NAME = os.environ.get("GCP_BUCKET_NAME", "projet6-raw-colin")
SOURCE_NAME = "yfinance"

# Colonnes attendues — validation de schema en entree
EXPECTED_COLUMNS = {"Open", "High", "Low", "Close", "Volume", "Adj Close"}


# ── Helpers ───────────────────────────────────────────────────────────────────
def get_target_date(date_str: str | None) -> date:
    """Retourne la date cible (aujourd'hui ou date passée en argument)."""
    if date_str:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    return date.today()


def is_market_open(target_date: date) -> bool:
    """Retourne False si weekend (samedi ou dimanche)."""
    return target_date.weekday() < 5  # 0=lundi ... 4=vendredi


def validate_schema(df: pd.DataFrame, ticker: str) -> bool:
    """Vérifie que les colonnes attendues sont présentes."""
    missing = EXPECTED_COLUMNS - set(df.columns)
    if missing:
        print(f"  ⚠️  {ticker} — colonnes manquantes : {missing}")
        return False
    return True


def validate_data(df: pd.DataFrame, ticker: str) -> bool:
    """Vérifie que les données sont cohérentes (pas vides, pas de prix nuls)."""
    if df.empty:
        print(f"  ⚠️  {ticker} — DataFrame vide")
        return False
    if (df["Close"] <= 0).any():
        print(f"  ⚠️  {ticker} — prix de clôture nul ou négatif détecté")
        return False
    return True


# ── Extraction ────────────────────────────────────────────────────────────────
def extract_ticker(ticker: str, target_date: date) -> pd.DataFrame | None:
    """
    Récupère les cours via Alpha Vantage TIME_SERIES_DAILY.
    Fallback depuis yfinance si Alpha Vantage échoue.
    """
    import requests

    # Convertir le ticker Yahoo Finance en ticker Alpha Vantage
    # BNP.PA → BNP.PAR (Euronext Paris)
    av_ticker_map = {
        "BNP.PA": "BNP.PAR",
        "GLE.PA": "GLE.PAR",
        "ACA.PA": "ACA.PAR",
    }
    av_ticker = av_ticker_map.get(ticker, ticker)
    api_key   = os.environ.get("ALPHA_VANTAGE_KEY", "")

    if api_key:
        try:
            url = "https://www.alphavantage.co/query"
            params = {
                "function":   "TIME_SERIES_DAILY",
                "symbol":     av_ticker,
                "apikey":     api_key,
                "outputsize": "compact",
            }
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            if "Note" in data:
                print(f"  ⚠️  {ticker} — Rate limit Alpha Vantage")
            elif "Time Series (Daily)" in data:
                ts = data["Time Series (Daily)"]
                date_str = str(target_date)

                if date_str in ts:
                    row = ts[date_str]
                else:
                    # Prendre la date la plus récente disponible
                    latest_date = sorted(ts.keys())[-1]
                    print(f"  ⚠️  {ticker} — {date_str} absent, utilisation de {latest_date}")
                    row = ts[latest_date]
                    date_str = latest_date

                df = pd.DataFrame([{
                    "ticker":        ticker,
                    "date_cotation": date_str,
                    "open":          float(row["1. open"]),
                    "high":          float(row["2. high"]),
                    "low":           float(row["3. low"]),
                    "close":         float(row["4. close"]),
                    "adj_close":     float(row["4. close"]),
                    "volume":        int(row["5. volume"]),
                }])
                print(f"  ✅ {ticker} — cours Alpha Vantage : {row['4. close']}")
                return df
        except Exception as e:
            print(f"  ⚠️  {ticker} — Alpha Vantage échoué : {e}, tentative yfinance...")

    # Fallback yfinance (fonctionne en local, pas en CI)
    try:
        start = target_date.strftime("%Y-%m-%d")
        end   = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")
        raw   = yf.download(ticker, start=start, end=end,
                            progress=False, auto_adjust=False)

        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)

        if not validate_schema(raw, ticker):
            return None
        if not validate_data(raw, ticker):
            return None

        df = raw.reset_index()
        df["ticker"] = ticker
        df = df.rename(columns={
            "Date": "date_cotation", "Open": "open", "High": "high",
            "Low": "low", "Close": "close", "Adj Close": "adj_close",
            "Volume": "volume"
        })
        df["date_cotation"] = pd.to_datetime(df["date_cotation"]).dt.date
        df = df[["ticker", "date_cotation", "open", "high",
                 "low", "close", "adj_close", "volume"]]

        print(f"  ✅ {ticker} — {len(df)} ligne(s) via yfinance")
        return df

    except Exception as e:
        print(f"  ❌ {ticker} — erreur : {e}")
        return None

# ── Upload GCS ────────────────────────────────────────────────────────────────
def upload_to_gcs(df: pd.DataFrame, target_date: date) -> tuple[bool, str]:
    """Dépose le CSV dans GCS sous gs://{BUCKET}/{source}/{YYYY-MM-DD}/cours.csv"""
    date_str = target_date.strftime("%Y-%m-%d")
    gcs_path = f"{SOURCE_NAME}/{date_str}/cours.csv"

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
    parser = argparse.ArgumentParser(description="Extrait les cours depuis Yahoo Finance")
    parser.add_argument("--date", type=str, default=None,
                        help="Date cible au format YYYY-MM-DD (défaut : aujourd'hui)")
    args = parser.parse_args()

    target_date = get_target_date(args.date)

    print(f"\n📈 Extraction Yahoo Finance — {target_date}")
    print(f"   Tickers : {TICKERS}")
    print(f"   Bucket  : {BUCKET_NAME}\n")

    # Weekend : marché fermé, pas d'erreur
    if not is_market_open(target_date):
        day_name = target_date.strftime("%A")
        print(f"⏭️  {day_name} — marché fermé, pas de données attendues.")
        try:
            from log_pipeline_run import log_run
            log_run(
                source_name       = SOURCE_NAME,
                step              = "extract",
                records_extracted = 0,
                records_loaded    = 0,
                duration_seconds  = 0.0,
                status            = "warning",
                error_message     = f"{day_name} — marché fermé"
            )
        except Exception:
            pass
        sys.exit(0)

    start_time = time.time()
    records_extracted = 0
    errors = []
    all_frames = []

    for i, ticker in enumerate(TICKERS):
        print(f"→ {ticker}")
        df = extract_ticker(ticker, target_date)

        if df is not None:
            all_frames.append(df)
            records_extracted += len(df)
        else:
            errors.append(ticker)

        if i < len(TICKERS) - 1:
            time.sleep(2)

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
                error_message = f"Tickers en erreur : {errors}"
        else:
            status = "failure"
            error_message = "Échec de l'upload GCS"
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
