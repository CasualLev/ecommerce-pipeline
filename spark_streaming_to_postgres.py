"""
Spark Structured Streaming job: watch data/input/ for new CSV files,
clean and validate the events, and write them to PostgreSQL.

Writes are idempotent: each micro-batch goes to a staging table, then
INSERT ... ON CONFLICT DO NOTHING copies it into events. A replayed
batch therefore never creates duplicates or fails on the primary key.

Usage:
    python spark_streaming_to_postgres.py
    python spark_streaming_to_postgres.py --trigger-seconds 2 --max-files-per-trigger 5
"""

import argparse
import csv
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

logger = logging.getLogger("streaming_job")

PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")

# ---------- configuration ----------

PG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "dbname": os.getenv("PG_DB", "ecommerce"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
}
JDBC_URL = f"jdbc:postgresql://{PG['host']}:{PG['port']}/{PG['dbname']}"
POSTGRES_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.3"

DEFAULT_INPUT_DIR = PROJECT_ROOT / "data" / "input"
DEFAULT_CHECKPOINT_DIR = PROJECT_ROOT / "checkpoints" / "events_stream"
REJECTED_DIR = PROJECT_ROOT / "data" / "rejected"
METRICS_FILE = PROJECT_ROOT / "logs" / "batch_metrics.csv"

EVENT_TYPES = ["view", "add_to_cart", "purchase"]

RAW_COLUMNS = [
    "event_id", "event_type", "user_id", "product_id", "product_name",
    "category", "price", "quantity", "event_timestamp",
]
# Read everything as text so we control (and can explain) every conversion
RAW_SCHEMA = StructType([StructField(c, StringType(), True) for c in RAW_COLUMNS])

TABLE_COLUMNS = [
    "event_id", "event_type", "user_id", "product_id", "product_name", "category",
    "price", "quantity", "total_amount", "event_timestamp", "source_file", "processed_at",
]

_cols = ", ".join(TABLE_COLUMNS)
MERGE_SQL = f"""
    INSERT INTO events ({_cols})
    SELECT {_cols} FROM events_staging
    ON CONFLICT (event_id) DO NOTHING;
"""

METRIC_FIELDS = [
    "batch_id", "batch_started_at", "input_rows", "valid_rows", "rejected_rows",
    "duplicates_removed", "inserted_rows", "already_existing",
    "processing_seconds", "rows_per_second",
]


# ---------- setup ----------

def create_spark() -> SparkSession:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    return (
        SparkSession.builder
        .appName("EcommerceEventsToPostgres")
        .master("local[*]")
        .config("spark.jars.packages", POSTGRES_JDBC_PACKAGE)  # downloads the JDBC driver
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def check_database() -> None:
    """Fail fast with a clear message if PostgreSQL isn't reachable."""
    try:
        conn = psycopg2.connect(**PG)
    except psycopg2.OperationalError as exc:
        raise SystemExit(f"Cannot connect to PostgreSQL ({PG['host']}:{PG['port']}): {exc}")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM events;")
            logger.info("Connected to PostgreSQL. events table currently has %d rows", cur.fetchone()[0])
    finally:
        conn.close()


# ---------- read & transform ----------

def read_event_stream(spark: SparkSession, input_dir: Path, max_files: int) -> DataFrame:
    return (
        spark.readStream
        .format("csv")
        .option("header", "true")
        .option("pathGlobFilter", "*.csv")          # ignore .tmp files from the generator
        .option("maxFilesPerTrigger", max_files)    # cap files per micro-batch
        .schema(RAW_SCHEMA)
        .load(str(input_dir))
    )


def _missing(column: str) -> Column:
    return F.col(column).isNull() | (F.col(column) == "")


def transform_events(df: DataFrame) -> DataFrame:
    """Clean, convert types, and tag each row with a reject reason (null = valid)."""
    typed = (
        df
        .withColumn("event_id", F.trim("event_id"))
        .withColumn("event_type", F.lower(F.trim("event_type")))  # "  VIEW " -> "view"
        .withColumn("user_id", F.trim("user_id").cast("int"))
        .withColumn("product_id", F.trim("product_id").cast("int"))
        .withColumn("product_name", F.trim("product_name"))
        .withColumn("category", F.trim("category"))
        .withColumn("price", F.trim("price").cast("double"))
        .withColumn("quantity", F.trim("quantity").cast("int"))
        .withColumn("event_timestamp",
                    F.to_timestamp(F.trim("event_timestamp"), "yyyy-MM-dd HH:mm:ss.SSS"))
        .withColumn("source_file", F.element_at(F.split(F.input_file_name(), "/"), -1))
    )

    reject_reason = (
        F.when(_missing("event_id"), "missing_event_id")
        .when(F.col("event_type").isNull() | ~F.col("event_type").isin(EVENT_TYPES), "invalid_event_type")
        .when(F.col("user_id").isNull(), "invalid_user_id")
        .when(F.col("product_id").isNull(), "invalid_product_id")
        .when(_missing("product_name") | _missing("category"), "missing_product_info")
        .when(F.col("price").isNull() | (F.col("price") < 0), "invalid_price")
        .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "invalid_quantity")
        .when(F.col("event_timestamp").isNull(), "invalid_timestamp")
    )

    return (
        typed
        .withColumn("reject_reason", reject_reason)
        .withColumn(
            "total_amount",
            F.when(F.col("event_type") == "purchase", F.round(F.col("price") * F.col("quantity"), 2))
            .otherwise(F.lit(0.0)),
        )
        .withColumn("processed_at", F.current_timestamp())
    )


# ---------- write ----------

def write_to_staging(df: DataFrame) -> None:
    (
        df.select(*TABLE_COLUMNS)
        .write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "events_staging")
        .option("user", PG["user"])
        .option("password", PG["password"])
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")   # with overwrite: empty the table, keep its definition
        .option("batchsize", 1000)
        .mode("overwrite")
        .save()
    )


def merge_staging_into_events() -> int:
    """Copy staged rows into events, skipping IDs that already exist. Returns rows inserted."""
    conn = psycopg2.connect(**PG)
    try:
        with conn, conn.cursor() as cur:  # 'with conn' commits on success, rolls back on error
            cur.execute(MERGE_SQL)
            return cur.rowcount
    finally:
        conn.close()


def append_metrics(row: dict) -> None:
    METRICS_FILE.parent.mkdir(parents=True, exist_ok=True)
    is_new = not METRICS_FILE.exists()
    with open(METRICS_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=METRIC_FIELDS)
        if is_new:
            writer.writeheader()
        writer.writerow(row)


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Called by Spark once per micro-batch."""
    started_at = datetime.now()
    start = time.perf_counter()

    batch_df.persist()  # reused several times below; compute once
    try:
        input_rows = batch_df.count()
        if input_rows == 0:
            return

        valid = batch_df.filter(F.col("reject_reason").isNull())
        rejected = batch_df.filter(F.col("reject_reason").isNotNull())

        reasons = {r["reject_reason"]: r["count"] for r in rejected.groupBy("reject_reason").count().collect()}
        rejected_rows = sum(reasons.values())
        if rejected_rows:
            rejected.write.mode("append").option("header", "true").csv(str(REJECTED_DIR))

        valid_rows = input_rows - rejected_rows
        unique = valid.dropDuplicates(["event_id"])
        unique_rows = unique.count()

        inserted = 0
        if unique_rows:
            write_to_staging(unique)
            inserted = merge_staging_into_events()

        seconds = time.perf_counter() - start
        metrics = {
            "batch_id": batch_id,
            "batch_started_at": started_at.isoformat(timespec="milliseconds"),
            "input_rows": input_rows,
            "valid_rows": valid_rows,
            "rejected_rows": rejected_rows,
            "duplicates_removed": valid_rows - unique_rows,
            "inserted_rows": inserted,
            "already_existing": unique_rows - inserted,
            "processing_seconds": round(seconds, 3),
            "rows_per_second": round(input_rows / seconds, 1) if seconds else 0,
        }
        append_metrics(metrics)

        logger.info(
            "Batch %d: %d in, %d inserted, %d rejected %s, %d duplicates removed, %.2fs",
            batch_id, input_rows, inserted, rejected_rows, reasons or "", metrics["duplicates_removed"], seconds,
        )
    finally:
        batch_df.unpersist()


# ---------- main ----------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream e-commerce event CSVs into PostgreSQL.")
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--checkpoint-dir", type=Path, default=DEFAULT_CHECKPOINT_DIR)
    parser.add_argument("--trigger-seconds", type=int, default=5, help="How often to check for new files")
    parser.add_argument("--max-files-per-trigger", type=int, default=10)
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()

    check_database()
    args.input_dir.mkdir(parents=True, exist_ok=True)

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    events = transform_events(read_event_stream(spark, args.input_dir, args.max_files_per_trigger))

    query = (
        events.writeStream
        .queryName("ecommerce_events_to_postgres")
        .foreachBatch(process_batch)
        .option("checkpointLocation", str(args.checkpoint_dir))
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )
    logger.info("Streaming started. Watching %s (Ctrl+C to stop)", args.input_dir)

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping stream...")
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()