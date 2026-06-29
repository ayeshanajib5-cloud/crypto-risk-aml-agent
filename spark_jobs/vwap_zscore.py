"""
Spark Streaming: VWAP + Z-Score AML Signal Processor

Reads from Kafka raw-trades topic, computes rolling VWAP and z-scores,
writes anomaly signals to market-risk-signals topic and PostgreSQL.

Target role: Scotia Bank Market Surveillance, Coinbase/Kraken FDE
Why this matters: VWAP deviation is the #1 indicator used by compliance
teams to detect wash trading and price manipulation under FINTRAC guidelines.
"""

import json
import logging
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, BooleanType, TimestampType
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("aml.spark")

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INPUT_TOPIC     = os.getenv("RAW_TRADES_TOPIC", "raw-trades")
OUTPUT_TOPIC    = os.getenv("RISK_SIGNALS_TOPIC", "market-risk-signals")
POSTGRES_URL    = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/crypto_risk")
POSTGRES_USER   = os.getenv("POSTGRES_USER", "riskadmin")
POSTGRES_PASS   = os.getenv("POSTGRES_PASSWORD", "changeme_in_prod")

# AML thresholds — these mirror real compliance parameters
# Z-score > 3: price is 3 standard deviations from VWAP = anomalous
# Z-score > 5: extreme anomaly = immediate escalation
ZSCORE_ALERT_THRESHOLD    = float(os.getenv("ZSCORE_ALERT_THRESHOLD", "3.0"))
ZSCORE_CRITICAL_THRESHOLD = float(os.getenv("ZSCORE_CRITICAL_THRESHOLD", "5.0"))

# ── Trade Schema ──────────────────────────────────────────────────────────────
TRADE_SCHEMA = StructType([
    StructField("event_type",      StringType(),  True),
    StructField("symbol",          StringType(),  True),
    StructField("trade_id",        LongType(),    True),
    StructField("price",           DoubleType(),  True),
    StructField("quantity",        DoubleType(),  True),
    StructField("notional_value",  DoubleType(),  True),
    StructField("is_buyer_maker",  BooleanType(), True),
    StructField("trade_time_ms",   LongType(),    True),
    StructField("trade_time_iso",  StringType(),  True),
    StructField("ingested_at",     StringType(),  True),
])

def create_spark_session():
    """
    Create SparkSession with Kafka connector.
    checkpoint_location stores streaming state between restarts —
    critical for exactly-once processing in compliance systems.
    """
    return (
        SparkSession.builder
        .appName("CryptoAML-VWAP-ZScore")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )

def write_to_kafka(df, topic):
    """Write risk signals back to Kafka as JSON."""
    return (
        df.select(
            F.to_json(F.struct("*")).alias("value")
        )
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", topic)
        .save()
    )

def write_to_postgres(df, table):
    """Write risk signals to PostgreSQL for compliance dashboard."""
    (
        df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", table)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASS)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

def process_batch(batch_df, batch_id):
    """
    Process each micro-batch of trades.

    For each batch we:
    1. Compute VWAP per symbol (sum(price*qty) / sum(qty))
    2. Compute mean and stddev of prices per symbol
    3. Calculate z-score for each trade: (price - mean) / stddev
    4. Flag trades where |z-score| > threshold
    5. Write signals to Kafka + PostgreSQL

    FINTRAC context: Any trade with z-score > 3 gets logged as a
    potential suspicious transaction for compliance review.
    """
    if batch_df.isEmpty():
        return

    log.info("Processing batch %d with %d trades", batch_id, batch_df.count())

    # ── Step 1: Compute VWAP per symbol ───────────────────────────────────────
    # VWAP = sum(price * quantity) / sum(quantity)
    # This is the industry standard price benchmark for surveillance
    vwap_df = batch_df.groupBy("symbol", "window").agg(
        (F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity"))
        .alias("vwap"),
        F.mean("price").alias("price_mean"),
        F.stddev("price").alias("price_stddev"),
        F.mean("quantity").alias("volume_mean"),
        F.stddev("quantity").alias("volume_stddev"),
        F.sum("quantity").alias("total_volume"),
        F.count("*").alias("trade_count"),
    )

    # ── Step 2: Join VWAP back to individual trades ───────────────────────────
    enriched_df = batch_df.join(
        vwap_df, on=["symbol", "window"], how="left"
    )

    # ── Step 3: Compute Z-Scores ───────────────────────────────────────────────
    # Z-score = (value - mean) / stddev
    # Measures how many standard deviations a trade is from normal
    signals_df = enriched_df.withColumn(
        "price_zscore",
        F.when(
            F.col("price_stddev") > 0,
            (F.col("price") - F.col("price_mean")) / F.col("price_stddev")
        ).otherwise(F.lit(0.0))
    ).withColumn(
        "volume_zscore",
        F.when(
            F.col("volume_stddev") > 0,
            (F.col("quantity") - F.col("volume_mean")) / F.col("volume_stddev")
        ).otherwise(F.lit(0.0))
    ).withColumn(
        "abs_price_zscore", F.abs(F.col("price_zscore"))
    ).withColumn(
        "is_anomalous",
        F.col("abs_price_zscore") > ZSCORE_ALERT_THRESHOLD
    ).withColumn(
        "severity",
        F.when(F.col("abs_price_zscore") > ZSCORE_CRITICAL_THRESHOLD, "CRITICAL")
         .when(F.col("abs_price_zscore") > ZSCORE_ALERT_THRESHOLD, "HIGH")
         .otherwise("NORMAL")
    ).withColumn(
        "alert_type",
        F.when(
            F.col("abs_price_zscore") > ZSCORE_ALERT_THRESHOLD,
            F.lit("PRICE_ANOMALY")
        ).otherwise(F.lit("NORMAL"))
    ).withColumn(
        "computed_at",
        F.lit(datetime.now(timezone.utc).isoformat())
    ).withColumn(
        "window_start", F.col("window.start").cast(StringType())
    ).withColumn(
        "window_end", F.col("window.end").cast(StringType())
    )

    # ── Step 4: Write ALL signals to Kafka ────────────────────────────────────
    output_cols = [
        "symbol", "trade_id", "price", "quantity", "notional_value",
        "vwap", "price_zscore", "volume_zscore", "is_anomalous",
        "severity", "alert_type", "window_start", "window_end",
        "trade_time_iso", "computed_at"
    ]

    output_df = signals_df.select(output_cols)

    try:
        write_to_kafka(output_df, OUTPUT_TOPIC)
        log.info("Batch %d: wrote %d signals to Kafka", batch_id, output_df.count())
    except Exception as e:
        log.error("Failed to write to Kafka: %s", e)

    # ── Step 5: Write anomalies to PostgreSQL ─────────────────────────────────
    anomalies_df = signals_df.filter(F.col("is_anomalous") == True)
    anomaly_count = anomalies_df.count()

    if anomaly_count > 0:
        log.warning(
            "BATCH %d: %d ANOMALOUS TRADES DETECTED",
            batch_id, anomaly_count
        )
        try:
            pg_df = anomalies_df.select(
                F.col("symbol"),
                F.col("window_start").cast(TimestampType()),
                F.col("window_end").cast(TimestampType()),
                F.col("vwap"),
                F.col("price_zscore"),
                F.col("volume_zscore"),
                F.col("trade_count"),
                F.col("total_volume"),
                F.col("is_anomalous"),
            )
            write_to_postgres(pg_df, "risk_signals")
            log.info("Wrote %d anomalies to PostgreSQL", anomaly_count)
        except Exception as e:
            log.error("Failed to write to PostgreSQL: %s", e)

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    log.info("Starting AML VWAP+ZScore processor...")
    log.info("Reading from Kafka topic: %s", INPUT_TOPIC)
    log.info("Writing signals to: %s", OUTPUT_TOPIC)
    log.info("Alert threshold: z-score > %.1f", ZSCORE_ALERT_THRESHOLD)

    # ── Read from Kafka ────────────────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 1000)
        .load()
    )

    # ── Parse JSON trades ──────────────────────────────────────────────────────
    trades_df = (
        raw_stream
        .select(
            F.from_json(
                F.col("value").cast("string"),
                TRADE_SCHEMA
            ).alias("trade")
        )
        .select("trade.*")
        .withColumn(
            "trade_time",
            (F.col("trade_time_ms") / 1000).cast(TimestampType())
        )
    )

    # ── 30-second tumbling window ──────────────────────────────────────────────
    # Groups trades into 30-second buckets per symbol
    # VWAP and z-scores computed within each window
    windowed_df = trades_df.withColumn(
        "window",
        F.window(F.col("trade_time"), "30 seconds")
    )

    # ── Write stream using foreachBatch ───────────────────────────────────────
    # foreachBatch gives us full DataFrame API in each micro-batch
    # This is the pattern used in production financial streaming systems
    query = (
        windowed_df
        .writeStream
        .foreachBatch(process_batch)
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", "/tmp/spark-checkpoints/vwap")
        .start()
    )

    log.info("Spark streaming query started. Waiting for data...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
