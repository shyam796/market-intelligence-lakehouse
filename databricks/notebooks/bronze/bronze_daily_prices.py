# Databricks notebook source
# bronze_daily_prices.py
# raw → bronze transform, merges into delta table

import logging
import json
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, LongType
)
from pyspark.sql.functions import (
    col, lit, to_date, current_timestamp,
    input_file_name
)
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.getOrCreate()

# ── config ────────────────────────────────────────────────────────────────────
STORAGE_ACCOUNT = dbutils.secrets.get(scope="keyvault-scope", key="adls-storage-account-name")
ACCESS_KEY      = dbutils.secrets.get(scope="keyvault-scope", key="adls-access-key")
CONTAINER       = "lakehouse"

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    ACCESS_KEY
)

RUN_DATE    = date.today().isoformat()
TICKERS     = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]
RAW_PATH    = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/alpha_vantage/daily_prices/{RUN_DATE}"
BRONZE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/daily_prices"

price_schema = StructType([
    StructField("1. open",   StringType(), True),
    StructField("2. high",   StringType(), True),
    StructField("3. low",    StringType(), True),
    StructField("4. close",  StringType(), True),
    StructField("5. volume", StringType(), True),
])

# ── functions ─────────────────────────────────────────────────────────────────

def read_raw_json(ticker: str, run_date: str):
    path = f"{RAW_PATH}/{ticker}_{run_date}.json"
    logger.info(f"reading {ticker} from {path}")
    # multiline=true - alpha vantage JSON spans multiple lines
    return spark.read.option("multiline", "true").json(path)


def parse_time_series(raw_df, ticker: str):
    # alpha vantage nests dates as keys - python parse avoids spark struct issue
    path = f"{RAW_PATH}/{ticker}_{RUN_DATE}.json"
    data = json.loads(dbutils.fs.head(path, 10000000))
    time_series = data.get("Time Series (Daily)", {})

    rows = []
    for price_date, prices in time_series.items():
        rows.append((
            ticker,
            price_date,
            float(prices["1. open"]),
            float(prices["2. high"]),
            float(prices["3. low"]),
            float(prices["4. close"]),
            int(prices["5. volume"]),
            RUN_DATE
        ))

    df = spark.createDataFrame(rows, schema=[
        "ticker", "price_date", "open", "high",
        "low", "close", "volume", "ingestion_date"
    ])

    return df.withColumn("price_date", to_date(col("price_date"), "yyyy-MM-dd")) \
             .withColumn("ingested_at", current_timestamp())


def merge_to_bronze(df, ticker: str) -> None:
    if DeltaTable.isDeltaTable(spark, BRONZE_PATH):
        delta_table = DeltaTable.forPath(spark, BRONZE_PATH)
        # merge on ticker + price_date - prevents duplicates on reruns
        delta_table.alias("existing").merge(
            df.alias("new"),
            """
            existing.ticker     = new.ticker AND
            existing.price_date = new.price_date
            """
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    else:
        # first run - create the table
        df.write.format("delta") \
            .partitionBy("ingestion_date") \
            .mode("overwrite") \
            .save(BRONZE_PATH)

    logger.info(f"{ticker} merge complete")


# ── main ──────────────────────────────────────────────────────────────────────

def run_bronze():
    logger.info(f"bronze job started - {RUN_DATE}")

    for ticker in TICKERS:
        try:
            raw_df  = read_raw_json(ticker, RUN_DATE)
            flat_df = parse_time_series(raw_df, ticker)
            merge_to_bronze(flat_df, ticker)
        except Exception as e:
            logger.error(f"{ticker} bronze failed: {e}")
            raise

    logger.info("bronze job finished")


run_bronze()