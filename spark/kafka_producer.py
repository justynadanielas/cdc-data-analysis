"""
Kafka Producer – CSV → Kafka
============================
Reads a CSV file in chunks and publishes each row as a JSON message to a
Kafka topic using ``kafka-python``'s ``KafkaProducer``.

Usage
-----
Run standalone:
    python spark/kafka_producer.py
    python spark/kafka_producer.py --input data/2013.csv --topic health-data-raw

Import programmatically:
    from spark.kafka_producer import produce_csv_to_kafka
    result = produce_csv_to_kafka("data/2013.csv")
"""

from __future__ import annotations

import argparse
import json
import os

import pandas as pd
from kafka import KafkaProducer

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
KAFKA_TOPIC: str = "health-data-raw"

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_INPUT_CSV = os.path.join(_PROJECT_ROOT, "data", "2013.csv")


# ---------------------------------------------------------------------------
# Core function
# ---------------------------------------------------------------------------

def produce_csv_to_kafka(
    csv_path: str = DEFAULT_INPUT_CSV,
    topic: str = KAFKA_TOPIC,
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    chunk_size: int = 10_000,
) -> dict:
    """Read a CSV file in chunks and publish each row as a JSON message to Kafka.

    Args:
        csv_path:          Path to the source CSV file.
        topic:             Kafka topic to publish messages to.
        bootstrap_servers: Comma-separated Kafka broker addresses.
        chunk_size:        Number of rows per chunk read from the CSV.

    Returns:
        XCom-compatible dict with keys ``records_sent``, ``topic``, ``chunks``.
    """
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # Improve throughput for bulk ingestion
        linger_ms=50,
        batch_size=65_536,
        compression_type="gzip",
    )

    records_sent = 0
    chunks = 0

    for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
        for record in chunk.to_dict(orient="records"):
            producer.send(topic, value=record)
        producer.flush()
        records_sent += len(chunk)
        chunks += 1
        print(f"[PRODUCER] chunk {chunks:4d} — {records_sent:,} records sent so far")

    producer.close()

    result = {"records_sent": records_sent, "topic": topic, "chunks": chunks}
    print(
        f"[PRODUCER] Done. {records_sent:,} records → topic '{topic}' "
        f"({chunks} chunk(s))"
    )
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish a CSV file row-by-row to a Kafka topic as JSON messages.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT_CSV,
        metavar="PATH",
        help="Path to the source CSV file.",
    )
    parser.add_argument(
        "--topic",
        default=KAFKA_TOPIC,
        help="Kafka topic to publish to.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=KAFKA_BOOTSTRAP_SERVERS,
        dest="bootstrap_servers",
        metavar="HOST:PORT",
        help="Kafka broker address(es), comma-separated.",
    )
    parser.add_argument(
        "--chunk-size",
        default=10_000,
        dest="chunk_size",
        type=int,
        metavar="N",
        help="Number of CSV rows per chunk.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    result = produce_csv_to_kafka(
        csv_path=args.input,
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        chunk_size=args.chunk_size,
    )
    print(
        f"\nProducer complete: {result['records_sent']:,} records sent "
        f"in {result['chunks']} chunk(s) to topic '{result['topic']}'"
    )


if __name__ == "__main__":
    main()
