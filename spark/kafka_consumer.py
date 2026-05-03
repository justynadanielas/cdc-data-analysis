"""
Kafka Consumer – Kafka → Bronze Parquet
========================================
Polls a Kafka topic until idle for ``idle_timeout_s`` seconds, accumulates all
rows, then writes them as a single Parquet file via PySpark.

Offset semantics
----------------
- ``enable_auto_commit=False`` — offsets are committed **only** after the
  Parquet write succeeds (at-least-once delivery guarantee).
- ``auto_offset_reset='earliest'`` — a fresh consumer group always replays
  the full topic, making each DAG run deterministic.

Usage
-----
Run standalone:
    python spark/kafka_consumer.py
    python spark/kafka_consumer.py --topic health-data-raw --output-dir data/pipeline_output

Import programmatically:
    from spark.kafka_consumer import consume_kafka_to_bronze
    bronze_path = consume_kafka_to_bronze(
        topic="health-data-raw",
        bootstrap_servers="localhost:9092",
        output_dir="data/pipeline_output",
        consumer_group="my-group",
    )
"""

from __future__ import annotations

import argparse
import json
import os
import time

import pandas as pd
from kafka import KafkaConsumer
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
KAFKA_TOPIC: str = "health-data-raw"
DEFAULT_CONSUMER_GROUP: str = "health_pipeline_bronze_consumer"

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_OUTPUT_DIR = os.path.join(_PROJECT_ROOT, "data", "pipeline_output")


# ---------------------------------------------------------------------------
# Core function
# ---------------------------------------------------------------------------

def consume_kafka_to_bronze(
    topic: str = KAFKA_TOPIC,
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    consumer_group: str = DEFAULT_CONSUMER_GROUP,
    idle_timeout_s: int = 60,
) -> str:
    """Consume a Kafka topic and write all messages as Bronze Parquet.

    Polls the topic in a loop.  When no new messages arrive for
    ``idle_timeout_s`` consecutive seconds the poll loop exits and the
    accumulated records are written to Parquet via a SparkSession.
    Offsets are committed only after the Parquet write succeeds
    (at-least-once semantics).

    Args:
        topic:             Kafka topic to consume from.
        bootstrap_servers: Comma-separated Kafka broker address(es).
        output_dir:        Base output directory.  The Parquet file is written
                           to ``<output_dir>/bronze/raw_data.parquet``.
        consumer_group:    Kafka consumer group ID.  Use a unique value per DAG
                           run to ensure each run consumes from the beginning
                           independently.
        idle_timeout_s:    Seconds without new messages before the consumer
                           stops polling and proceeds to write Parquet.

    Returns:
        Absolute path to the written Parquet directory.

    Raises:
        RuntimeError: If no messages are found on the topic after the first
                      poll attempt, or if Kafka is unreachable.
    """
    output_path = os.path.join(output_dir, "bronze", "raw_data.parquet")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=consumer_group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        # poll() call timeout – short to react quickly to idle condition
        consumer_timeout_ms=1_000,
    )

    print(
        f"[CONSUMER] Subscribed to '{topic}' as group '{consumer_group}'. "
        f"Idle timeout: {idle_timeout_s}s"
    )

    records: list[dict] = []
    last_message_time = time.monotonic()

    try:
        while True:
            # poll() returns {TopicPartition: [ConsumerRecord, ...]}
            batch = consumer.poll(timeout_ms=1_000)

            if batch:
                for partition_records in batch.values():
                    for msg in partition_records:
                        records.append(msg.value)
                last_message_time = time.monotonic()
                print(
                    f"[CONSUMER] {len(records):,} records accumulated …",
                    end="\r",
                    flush=True,
                )
            else:
                idle_secs = time.monotonic() - last_message_time
                if idle_secs >= idle_timeout_s:
                    print(
                        f"\n[CONSUMER] No messages for {idle_secs:.1f}s "
                        f"(>= {idle_timeout_s}s) — stopping poll loop."
                    )
                    break

        if not records:
            raise RuntimeError(
                f"No messages found on topic '{topic}'. "
                "Run the producer first (spark/kafka_producer.py)."
            )

        print(f"[CONSUMER] {len(records):,} total records — writing Parquet …")

        spark = SparkSession.builder.appName("kafka_consumer_bronze").getOrCreate()
        df = spark.createDataFrame(pd.DataFrame(records))
        df.write.mode("overwrite").parquet(output_path)

        # Commit offsets only after successful Parquet write (at-least-once)
        consumer.commit()

        row_count = df.count()
        print(f"[CONSUMER] {row_count:,} records written → {output_path}")

    finally:
        consumer.close()

    return output_path


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume a Kafka topic and write Bronze Parquet via PySpark."
    )
    parser.add_argument(
        "--topic",
        default=KAFKA_TOPIC,
        help=f"Kafka topic to consume (default: {KAFKA_TOPIC})",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=KAFKA_BOOTSTRAP_SERVERS,
        dest="bootstrap_servers",
        help=f"Kafka broker(s) (default: {KAFKA_BOOTSTRAP_SERVERS})",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        dest="output_dir",
        help=f"Base output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--consumer-group",
        default=DEFAULT_CONSUMER_GROUP,
        dest="consumer_group",
        help=f"Kafka consumer group ID (default: {DEFAULT_CONSUMER_GROUP})",
    )
    parser.add_argument(
        "--idle-timeout",
        type=int,
        default=60,
        dest="idle_timeout_s",
        help="Seconds without new messages before stopping (default: 60)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    bronze_path = consume_kafka_to_bronze(
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        output_dir=args.output_dir,
        consumer_group=args.consumer_group,
        idle_timeout_s=args.idle_timeout_s,
    )
    print(f"[CONSUMER] Done. Bronze path: {bronze_path}")
