"""
Health Data Pipeline – Standalone Spark Script
===============================================
Single entry point that runs the full Bronze → Silver → Gold pipeline.
Processing is done outside the database using Apache Spark with Parquet files.

Layers
------
Bronze – raw CSV ingested as-is (no schema changes)
Silver – cleaned and semantically labelled data (invalid rows dropped)
Gold   – state-level aggregates ready for analytics

Usage
-----
Run locally via Python:
    python spark/health_data_pipeline.py
    python spark/health_data_pipeline.py --input data/2013.csv --output-dir data/pipeline_output

Run on a Spark cluster:
    spark-submit spark/health_data_pipeline.py [--input PATH] [--output-dir PATH]
"""

from __future__ import annotations

import argparse
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Default paths – resolved relative to the project root (parent of spark/)
# ---------------------------------------------------------------------------
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_INPUT_CSV = os.path.join(_PROJECT_ROOT, "data", "2013.csv")
DEFAULT_OUTPUT_DIR = os.path.join(_PROJECT_ROOT, "data", "pipeline_output")
DEFAULT_REF_STATE_CODES_CSV = os.path.join(_PROJECT_ROOT, "data", "ref_state_codes.csv")

# ---------------------------------------------------------------------------
# Kafka defaults – overridable via the KAFKA_BOOTSTRAP_SERVERS environment
# variable so no code changes are needed to point at an external cluster.
# ---------------------------------------------------------------------------
DEFAULT_KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
DEFAULT_KAFKA_TOPIC: str = "health-data-raw"


# ---------------------------------------------------------------------------
# Kafka → Bronze function  (new path; does NOT replace run_bronze)
# ---------------------------------------------------------------------------

def run_bronze_from_kafka(
    spark: SparkSession,
    output_dir: str,
    bootstrap_servers: str = DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
    topic: str = DEFAULT_KAFKA_TOPIC,
    group_id: str = "health_pipeline_bronze_consumer",
) -> str:
    """Consume all messages from a Kafka topic and write them as Bronze Parquet.

    Uses ``kafka-python``'s ``KafkaConsumer`` (not spark-sql-kafka) to avoid
    JAR version-compatibility issues with PySpark 4.x.  Suitable for
    development / moderate-scale workloads.

    The consumer always starts from the beginning of the topic
    (``auto_offset_reset='earliest'``) and stops once it has caught up to the
    latest offset, making the operation deterministic and idempotent for a
    given topic state.

    Args:
        spark:             Active SparkSession.
        output_dir:        Base output directory; a ``bronze/`` subdirectory is
                           created (same path as ``run_bronze``).
        bootstrap_servers: Comma-separated Kafka broker addresses.
        topic:             Kafka topic to consume from.
        group_id:          Kafka consumer group ID.  Use a unique value per DAG
                           run if you need each run to consume independently.

    Returns:
        Path to the written Parquet directory.

    Raises:
        RuntimeError: If no messages are found on the topic.
    """
    # Import here so that environments without kafka-python installed can still
    # use the direct-CSV path without ImportError at module load time.
    from kafka import KafkaConsumer  # noqa: PLC0415
    from kafka import TopicPartition  # noqa: PLC0415

    output_path = os.path.join(output_dir, "bronze", "raw_data.parquet")

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        # Stop polling once we reach the end of the partition
        consumer_timeout_ms=5_000,
    )
    consumer.subscribe([topic])

    # Force partition assignment by polling once, then seek to beginning so we
    # always replay the full topic (idempotent behaviour).
    consumer.poll(timeout_ms=2_000)
    partitions = consumer.assignment()
    if not partitions:
        consumer.close()
        raise RuntimeError(
            f"No partitions assigned for topic '{topic}'. "
            "Ensure Kafka is running and the topic exists."
        )
    consumer.seek_to_beginning(*partitions)

    records: list[dict] = []
    for msg in consumer:
        records.append(msg.value)

    consumer.close()

    if not records:
        raise RuntimeError(
            f"No messages found on topic '{topic}'. "
            "Run the producer first (spark/kafka_producer.py)."
        )

    import pandas as pd  # noqa: PLC0415 – pandas already in requirements.txt
    df = spark.createDataFrame(pd.DataFrame(records))
    # df = spark.createDataFrame(records)
    df.write.mode("overwrite").parquet(output_path)
    print(f"[BRONZE/KAFKA] {df.count():,} records written → {output_path}")
    return output_path


def get_spark_session(app_name: str = "health_data_pipeline") -> SparkSession:
    """Return (or create) a SparkSession with the given application name.

    When called from within an existing Airflow/Spark context the existing
    session is reused.  When called from the CLI a new local session is started.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()


# ---------------------------------------------------------------------------
# Layer functions – each is idempotent via write.mode("overwrite")
# ---------------------------------------------------------------------------

def run_bronze(spark: SparkSession, input_csv: str, output_dir: str) -> str:
    """Ingest raw CSV data into the Bronze layer without any transformations.

    Args:
        spark:      Active SparkSession.
        input_csv:  Path to the source CSV file.
        output_dir: Base output directory; a ``bronze/`` subdirectory is created.

    Returns:
        Path to the written Parquet directory.
    """
    output_path = os.path.join(output_dir, "bronze", "raw_data.parquet")
    df = spark.read.csv(input_csv, header=True, inferSchema=True)
    df.write.mode("overwrite").parquet(output_path)
    print(f"[BRONZE] {df.count():,} records written → {output_path}")
    return output_path


def run_silver(spark: SparkSession, bronze_path: str, output_dir: str) -> str:
    """Clean and semantically enrich Bronze data into the Silver layer.

    Transformations applied:
    - _STATE          → State_Code               (kept as-is)
    - GENHLTH (1–5)   → General_Health label      (rows outside 1–5 are dropped)
    - _BMI5 (int ×100) → BMI_Value decimal        (null / zero rows are dropped)
    - _TOTINDA (1–2)  → Physical_Activity_Status  ("Active" / "Inactive" / "Unknown")
    - _AGEG5YR        → Age_Group_Code            (kept as-is)
    - _RACE           → Race_Code                 (kept as-is)

    Args:
        spark:       Active SparkSession.
        bronze_path: Path to Bronze Parquet output.
        output_dir:  Base output directory; a ``silver/`` subdirectory is created.

    Returns:
        Path to the written Parquet directory.
    """
    output_path = os.path.join(output_dir, "silver", "filtered_data.parquet")
    df = spark.read.parquet(bronze_path)

    transformed = (
        df.select(
            F.col("_STATE").alias("State_Code"),
            F.when(F.col("GENHLTH") == 1, "Excellent")
             .when(F.col("GENHLTH") == 2, "Very Good")
             .when(F.col("GENHLTH") == 3, "Good")
             .when(F.col("GENHLTH") == 4, "Fair")
             .when(F.col("GENHLTH") == 5, "Poor")
             .otherwise(None)
             .alias("General_Health"),
            (F.col("_BMI5").cast("decimal(10,2)") / 100).alias("BMI_Value"),
            F.when(F.col("_TOTINDA") == 1, "Active")
             .when(F.col("_TOTINDA") == 2, "Inactive")
             .otherwise("Unknown")
             .alias("Physical_Activity_Status"),
            F.col("_AGEG5YR").alias("Age_Group_Code"),
            F.col("_RACE").alias("Race_Code"),
        )
        .filter(
            (F.col("GENHLTH") <= 5)
            & F.col("_BMI5").isNotNull()
            & (F.col("_BMI5") > 0)
        )
    )

    transformed.write.mode("overwrite").parquet(output_path)
    print(f"[SILVER] {transformed.count():,} records written → {output_path}")
    return output_path


def run_gold(
    spark: SparkSession,
    silver_path: str,
    output_dir: str,
    ref_state_codes_csv: str = DEFAULT_REF_STATE_CODES_CSV,
) -> str:
    """Aggregate Silver data by state into the Gold analytics layer.

    Aggregations per State_Code:
    - record_count: number of survey respondents
    - bmi_sum:      rounded total BMI across respondents
    - bmi_mean:     rounded average BMI across respondents

    A ``State_Name`` column is added by joining with ``ref_state_codes_csv``
    on ``State_Code``.  Rows whose code has no matching entry in the reference
    file receive ``null`` for ``State_Name``.

    Args:
        spark:              Active SparkSession.
        silver_path:        Path to Silver Parquet output.
        output_dir:         Base output directory; a ``gold/`` subdirectory is created.
        ref_state_codes_csv: Path to the state-code reference CSV
                             (columns: ``state_code``, ``state_name``).

    Returns:
        Path to the written Parquet directory.
    """
    output_path = os.path.join(output_dir, "gold", "aggregated_by_state.parquet")
    df = spark.read.parquet(silver_path)

    aggregated = (
        df.groupBy("State_Code")
        .agg(
            F.count("BMI_Value").alias("record_count"),
            F.round(F.sum("BMI_Value"), 2).alias("bmi_sum"),
            F.round(F.mean("BMI_Value"), 4).alias("bmi_mean"),
        )
        .orderBy("State_Code")
    )

    ref_df = spark.read.csv(ref_state_codes_csv, header=True, inferSchema=True)
    aggregated = (
        aggregated
        .join(ref_df, aggregated["State_Code"] == ref_df["state_code"], how="left")
        .select(
            aggregated["State_Code"],
            F.col("state_name").alias("State_Name"),
            F.col("record_count"),
            F.col("bmi_sum"),
            F.col("bmi_mean"),
        )
        .orderBy("State_Code")
    )

    aggregated.write.mode("overwrite").parquet(output_path)
    print(f"[GOLD]   {aggregated.count():,} state aggregates written → {output_path}")
    return output_path


# ---------------------------------------------------------------------------
# Pipeline orchestration – single SparkSession shared across all layers
# ---------------------------------------------------------------------------

def run_pipeline(
    input_csv: str = DEFAULT_INPUT_CSV,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    ref_state_codes_csv: str = DEFAULT_REF_STATE_CODES_CSV,
) -> dict[str, str]:
    """Run the full Bronze → Silver → Gold pipeline in a single SparkSession.

    The SparkSession is always stopped on completion (or on error) when called
    from this function, so it is safe to use as a standalone entry point.
    Call the individual ``run_bronze`` / ``run_silver`` / ``run_gold`` functions
    directly when embedding the pipeline inside Airflow tasks (where the session
    lifecycle is managed externally).

    Args:
        input_csv:           Path to the source CSV file.
        output_dir:          Base directory for all pipeline layer outputs.
        ref_state_codes_csv: Path to the state-code reference CSV.

    Returns:
        Dict mapping each layer name (``bronze``, ``silver``, ``gold``) to its
        output path.
    """
    spark = get_spark_session("health_data_pipeline")
    print(f"\nPipeline starting")
    print(f"  input:      {input_csv}")
    print(f"  output dir: {output_dir}\n")
    try:
        bronze_path = run_bronze(spark, input_csv, output_dir)
        silver_path = run_silver(spark, bronze_path, output_dir)
        gold_path = run_gold(spark, silver_path, output_dir, ref_state_codes_csv)
    finally:
        spark.stop()

    return {"bronze": bronze_path, "silver": silver_path, "gold": gold_path}


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Health Data Pipeline – processes BRFSS health survey CSV data "
            "through Bronze → Silver → Gold layers using Apache Spark."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT_CSV,
        metavar="PATH",
        help="Path to source CSV file.",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        dest="output_dir",
        metavar="PATH",
        help="Base output directory for all pipeline layers.",
    )
    parser.add_argument(
        "--ref-state-codes",
        default=DEFAULT_REF_STATE_CODES_CSV,
        dest="ref_state_codes",
        metavar="PATH",
        help="Path to state-code reference CSV (columns: state_code, state_name).",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Single entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Parse CLI arguments and run the full pipeline."""
    args = _parse_args()
    output_paths = run_pipeline(args.input, args.output_dir, args.ref_state_codes)
    print("\nPipeline complete. Output paths:")
    for layer, path in output_paths.items():
        print(f"  {layer:6s}: {path}")


if __name__ == "__main__":
    main()
