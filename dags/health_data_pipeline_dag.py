# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
### Health Data Pipeline DAG

Orchestrates Bronze → Silver → Gold health data processing (BRFSS 2013 survey).
All Spark processing logic lives in spark/health_data_pipeline.py.

To run standalone (no Airflow needed):
    python spark/health_data_pipeline.py
    python spark/health_data_pipeline.py --input data/2013.csv --output-dir data/pipeline_output

To trigger via Airflow CLI:
    airflow dags trigger health_data_pipeline
"""

from __future__ import annotations

import os
import sys

import pendulum

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

# ---------------------------------------------------------------------------
# Add project root to sys.path so tasks can import spark.health_data_pipeline
# ---------------------------------------------------------------------------
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from spark.health_data_pipeline import (  # noqa: E402
    DEFAULT_INPUT_CSV,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_REF_STATE_CODES_CSV,
    get_spark_session,
    run_bronze,
    run_silver,
    run_gold,
)


def _path_safe_run_id(run_id: str) -> str:
    """Sanitize an Airflow run_id so it is safe to embed in a file-system path."""
    return run_id.replace(":", "-").replace("+", "").replace("/", "__")


with DAG(
    dag_id="health_data_pipeline",
    default_args={"retries": 1},
    description="Bronze → Silver → Gold health data pipeline (BRFSS 2013 survey).",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["health", "brfss", "spark"],
) as dag:
    dag.doc_md = __doc__

    def task_ingest_raw_data(**kwargs) -> None:
        """Bronze: read raw CSV → Parquet. Idempotent – overwrites output on re-run."""
        safe_rid = _path_safe_run_id(kwargs["dag_run"].run_id)
        output_dir = os.path.join(DEFAULT_OUTPUT_DIR, safe_rid)
        spark = get_spark_session("health_pipeline_bronze")
        bronze_path = run_bronze(spark, DEFAULT_INPUT_CSV, output_dir)
        kwargs["ti"].xcom_push(key="output_dir", value=output_dir)
        kwargs["ti"].xcom_push(key="bronze_path", value=bronze_path)

    def task_transform_to_silver(**kwargs) -> None:
        """Silver: clean and enrich Bronze data. Idempotent – overwrites output on re-run."""
        ti = kwargs["ti"]
        bronze_path = ti.xcom_pull(task_ids="ingest_raw_data", key="bronze_path")
        output_dir = ti.xcom_pull(task_ids="ingest_raw_data", key="output_dir")
        spark = get_spark_session("health_pipeline_silver")
        silver_path = run_silver(spark, bronze_path, output_dir)
        ti.xcom_push(key="silver_path", value=silver_path)

    def task_aggregate_to_gold(**kwargs) -> None:
        """Gold: aggregate Silver data by state and persist results. Idempotent – overwrites output on re-run."""
        ti = kwargs["ti"]
        silver_path = ti.xcom_pull(task_ids="transform_to_silver", key="silver_path")
        output_dir = ti.xcom_pull(task_ids="ingest_raw_data", key="output_dir")
        spark = get_spark_session("health_pipeline_gold")
        run_gold(spark, silver_path, output_dir, DEFAULT_REF_STATE_CODES_CSV)

    ingest_task = PythonOperator(
        task_id="ingest_raw_data",
        python_callable=task_ingest_raw_data,
    )

    transform_task = PythonOperator(
        task_id="transform_to_silver",
        python_callable=task_transform_to_silver,
    )

    aggregate_task = PythonOperator(
        task_id="aggregate_to_gold",
        python_callable=task_aggregate_to_gold,
    )

    ingest_task >> transform_task >> aggregate_task
