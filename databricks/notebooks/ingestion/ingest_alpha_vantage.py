# Databricks notebook source
# ingest_alpha_vantage.py
# pulls daily OHLCV prices from Alpha Vantage, writes raw JSON to ADLS

import requests
import json
import time
import logging
from datetime import datetime, date
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType,
    TimestampType, IntegerType, DoubleType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.getOrCreate()

# ── config ────────────────────────────────────────────────────────────────────
API_KEY        = dbutils.secrets.get(scope="keyvault-scope", key="alpha-vantage-api-key")
STORAGE_ACCOUNT = dbutils.secrets.get(scope="keyvault-scope", key="adls-storage-account-name")
ACCESS_KEY     = dbutils.secrets.get(scope="keyvault-scope", key="adls-access-key")
CONTAINER      = "lakehouse"
BASE_URL       = "https://www.alphavantage.co/query"

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    ACCESS_KEY
)

TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]

RAW_BASE_PATH  = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/alpha_vantage/daily_prices"
LOGS_PATH      = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/logs/pipeline_runs"

pipeline_runs_schema = StructType([
    StructField("run_id",         StringType(),    False),
    StructField("run_timestamp",  TimestampType(), False),
    StructField("ticker",         StringType(),    False),
    StructField("status",         StringType(),    False),
    StructField("rows_ingested",  IntegerType(),   True),
    StructField("duration_secs",  DoubleType(),    True),
    StructField("error_message",  StringType(),    True),
    StructField("run_date",       StringType(),    False),
])

# ── functions ─────────────────────────────────────────────────────────────────

def fetch_daily_prices(ticker: str, api_key: str, retries: int = 3) -> dict:
    # retries with exponential backoff - alpha vantage rate limits on free tier
    params = {
        "function":   "TIME_SERIES_DAILY",
        "symbol":     ticker,
        "outputsize": "compact",
        "apikey":     api_key,
    }

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"fetching {ticker} - attempt {attempt}")
            response = requests.get(BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            # alpha vantage returns 200 even on errors
            if "Error Message" in data:
                raise ValueError(f"api error for {ticker}: {data['Error Message']}")
            if "Note" in data:
                raise ValueError(f"rate limit hit for {ticker}: {data['Note']}")

            logger.info(f"{ticker} fetched OK")
            return data

        except Exception as e:
            logger.warning(f"{ticker} attempt {attempt} failed: {e}")
            if attempt < retries:
                wait = 2 ** attempt
                logger.info(f"waiting {wait}s before retry")
                time.sleep(wait)
            else:
                raise


def write_raw_json(data: dict, ticker: str, run_date: str) -> str:
    # overwrite=True - safe, raw zone is immutable landing area
    path = f"{RAW_BASE_PATH}/{run_date}/{ticker}_{run_date}.json"
    json_string = json.dumps(data, indent=2)
    dbutils.fs.put(path, json_string, overwrite=True)
    logger.info(f"raw json written to {path}")
    return path


def count_rows_in_response(data: dict) -> int:
    time_series = data.get("Time Series (Daily)", {})
    return len(time_series)


def log_pipeline_run(
    run_id: str,
    ticker: str,
    status: str,
    rows_ingested: int,
    duration_secs: float,
    error_message: str,
    run_date: str,
) -> None:
    # merge on run_id + ticker - prevents duplicates on reruns
    run_timestamp = datetime.utcnow()

    row = [(
        run_id,
        run_timestamp,
        ticker,
        status,
        rows_ingested,
        round(duration_secs, 3),
        error_message,
        run_date,
    )]

    df = spark.createDataFrame(row, schema=pipeline_runs_schema)

    if DeltaTable.isDeltaTable(spark, LOGS_PATH):
        delta_table = DeltaTable.forPath(spark, LOGS_PATH)
        delta_table.alias("existing").merge(
            df.alias("new"),
            "existing.run_id = new.run_id AND existing.ticker = new.ticker"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        df.write.format("delta") \
            .partitionBy("run_date") \
            .mode("overwrite") \
            .save(LOGS_PATH)

    logger.info(f"pipeline_runs logged - {ticker} {status}")


# ── main ──────────────────────────────────────────────────────────────────────

def run_ingestion():
    run_date = date.today().isoformat()
    logger.info(f"ingestion started - {run_date} - {len(TICKERS)} tickers")

    for ticker in TICKERS:
        run_id        = f"{ticker}_{run_date}"
        start_time    = time.time()
        status        = "success"
        rows_ingested = 0
        error_message = None

        try:
            data          = fetch_daily_prices(ticker, API_KEY)
            rows_ingested = count_rows_in_response(data)
            write_raw_json(data, ticker, run_date)

        except Exception as e:
            status        = "failure"
            error_message = str(e)
            logger.error(f"{ticker} ingestion failed: {e}")

        finally:
            duration_secs = time.time() - start_time
            log_pipeline_run(
                run_id        = run_id,
                ticker        = ticker,
                status        = status,
                rows_ingested = rows_ingested,
                duration_secs = duration_secs,
                error_message = error_message,
                run_date      = run_date,
            )

    logger.info("ingestion finished")


run_ingestion()