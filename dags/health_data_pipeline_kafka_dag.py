"""
### Health Data Pipeline – Kafka Ingestion DAG

Orchestrates CSV → Kafka → Bronze → Silver → Gold health data processing.

Task chain
----------
produce_to_kafka → consume_from_kafka_to_bronze → transform_to_silver → aggregate_to_gold

The producer and consumer steps use the kafka-python modules in spark/.
Silver and Gold reuse run_silver() / run_gold() from spark/health_data_pipeline
— no duplication with the direct-ingest DAG.

To trigger via Airflow CLI:
    airflow dags trigger health_data_pipeline_kafka
"""

from __future__ import annotations

import os
import sys

import pendulum

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

# ---------------------------------------------------------------------------
# Add project root to sys.path so tasks can import spark.*
# ---------------------------------------------------------------------------
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from spark.health_data_pipeline import (  # noqa: E402
    DEFAULT_INPUT_CSV,
    DEFAULT_OUTPUT_DIR,
    get_spark_session,
    run_silver,
    run_gold,
)
from spark.kafka_producer import (  # noqa: E402
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    produce_csv_to_kafka,
)
from spark.kafka_consumer import consume_kafka_to_bronze  # noqa: E402


def _path_safe_run_id(run_id: str) -> str:
    """Sanitize an Airflow run_id so it is safe to embed in a file-system path
    and use as a Kafka consumer group ID."""
    return run_id.replace(":", "-").replace("+", "").replace("/", "__")


with DAG(
    dag_id="health_data_pipeline_kafka",
    default_args={"retries": 1},
    description="CSV → Kafka → Bronze → Silver → Gold health data pipeline (BRFSS 2013).",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["health", "brfss", "spark", "kafka"],
) as dag:
    dag.doc_md = __doc__

    # -------------------------------------------------------------------------
    # Task 1 – Produce CSV rows to Kafka
    # -------------------------------------------------------------------------
    def task_produce_to_kafka(**kwargs) -> None:
        """Read source CSV and publish every row as a JSON message to Kafka."""
        result = produce_csv_to_kafka(
            csv_path=DEFAULT_INPUT_CSV,
            topic=KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            max_records=5_000,
        )
        kwargs["ti"].xcom_push(key="producer_result", value=result)

    # -------------------------------------------------------------------------
    # Task 2 – Consume Kafka → Bronze Parquet
    # -------------------------------------------------------------------------
    def task_consume_from_kafka_to_bronze(**kwargs) -> None:
        """Consume the Kafka topic and write Bronze Parquet.

        A unique consumer group per run_id prevents NotCoordinatorForGroupError
        when the same DAG is re-triggered.
        """
        safe_rid = _path_safe_run_id(kwargs["dag_run"].run_id)
        output_dir = os.path.join(DEFAULT_OUTPUT_DIR, safe_rid)
        consumer_group = f"health_kafka_{safe_rid}"

        bronze_path = consume_kafka_to_bronze(
            topic=KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            output_dir=output_dir,
            consumer_group=consumer_group,
            idle_timeout_s=60,
        )
        kwargs["ti"].xcom_push(key="output_dir", value=output_dir)
        kwargs["ti"].xcom_push(key="bronze_path", value=bronze_path)

    # -------------------------------------------------------------------------
    # Task 3 – Transform Bronze → Silver
    # -------------------------------------------------------------------------
    def task_transform_to_silver(**kwargs) -> None:
        """Clean and enrich Bronze data into the Silver layer."""
        ti = kwargs["ti"]
        bronze_path = ti.xcom_pull(task_ids="consume_from_kafka_to_bronze", key="bronze_path")
        output_dir = ti.xcom_pull(task_ids="consume_from_kafka_to_bronze", key="output_dir")

        if not os.path.exists(bronze_path):
            raise FileNotFoundError(
                f"Bronze Parquet not found at '{bronze_path}'. "
                "Check that consume_from_kafka_to_bronze succeeded."
            )

        spark = get_spark_session("health_kafka_silver")
        silver_path = run_silver(spark, bronze_path, output_dir)
        ti.xcom_push(key="silver_path", value=silver_path)

    # -------------------------------------------------------------------------
    # Task 4 – Aggregate Silver → Gold
    # -------------------------------------------------------------------------
    def task_aggregate_to_gold(**kwargs) -> None:
        """Aggregate Silver data by state into the Gold analytics layer."""
        ti = kwargs["ti"]
        silver_path = ti.xcom_pull(task_ids="transform_to_silver", key="silver_path")
        output_dir = ti.xcom_pull(task_ids="consume_from_kafka_to_bronze", key="output_dir")

        if not os.path.exists(silver_path):
            raise FileNotFoundError(
                f"Silver Parquet not found at '{silver_path}'. "
                "Check that transform_to_silver succeeded."
            )

        spark = get_spark_session("health_kafka_gold")
        run_gold(spark, silver_path, output_dir)

    # -------------------------------------------------------------------------
    # Wire up tasks
    # -------------------------------------------------------------------------
    produce_task = PythonOperator(
        task_id="produce_to_kafka",
        python_callable=task_produce_to_kafka,
    )

    consume_task = PythonOperator(
        task_id="consume_from_kafka_to_bronze",
        python_callable=task_consume_from_kafka_to_bronze,
    )

    transform_task = PythonOperator(
        task_id="transform_to_silver",
        python_callable=task_transform_to_silver,
    )

    aggregate_task = PythonOperator(
        task_id="aggregate_to_gold",
        python_callable=task_aggregate_to_gold,
    )

    produce_task >> consume_task >> transform_task >> aggregate_task
