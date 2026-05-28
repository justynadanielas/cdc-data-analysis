"""Dagster software-defined assets for the Bronze → Silver → Gold pipeline.

Each asset wraps the corresponding function from spark/health_data_pipeline.py
so the identical Spark transformations are reused verbatim.  Outputs are
written under:

    data/pipeline_output/dagster/<dagster_run_id>/{bronze,silver,gold}/

This keeps Dagster runs isolated from Airflow runs (which write to
data/pipeline_output/<airflow_run_id>/) and allows run-by-run comparison.
"""

import os
import sys

from dagster import OpExecutionContext, asset

# ---------------------------------------------------------------------------
# Add project root to sys.path so we can import spark.health_data_pipeline
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
    run_gold,
    run_silver,
)

# Dagster outputs land under a "dagster/" subdirectory so they never collide
# with Airflow outputs which use data/pipeline_output/<run_id>/.
DAGSTER_BASE_OUTPUT_DIR = os.path.join(DEFAULT_OUTPUT_DIR, "dagster")


# ---------------------------------------------------------------------------
# Bronze asset – raw CSV ingested as-is
# ---------------------------------------------------------------------------

@asset(
    group_name="health_pipeline",
    compute_kind="spark",
    description="Ingest raw CSV → Bronze Parquet without any transformations.",
)
def bronze_raw_data(context: OpExecutionContext) -> str:
    """Read the source CSV and write it as Bronze Parquet.

    Returns the full path to the written Parquet directory so that
    silver_filtered_data can use it as a typed upstream dependency.
    """
    run_output_dir = os.path.join(DAGSTER_BASE_OUTPUT_DIR, context.run_id)
    spark = get_spark_session("dagster_health_pipeline_bronze")
    bronze_path = run_bronze(spark, DEFAULT_INPUT_CSV, run_output_dir)
    context.log.info("Bronze layer written to: %s", bronze_path)
    return bronze_path


# ---------------------------------------------------------------------------
# Silver asset – cleaned and semantically labelled data
# ---------------------------------------------------------------------------

@asset(
    group_name="health_pipeline",
    compute_kind="spark",
    description="Clean and enrich Bronze data → Silver Parquet (invalid rows dropped).",
)
def silver_filtered_data(
    context: OpExecutionContext,
    bronze_raw_data: str,
) -> str:
    """Transform Bronze Parquet into Silver Parquet.

    Accepts the Bronze output path from the upstream bronze_raw_data asset.
    The run-scoped output directory is derived from the bronze path so that
    both layers land under the same run directory.

    Returns the full path to the written Parquet directory.
    """
    # bronze_path  = <base>/dagster/<run_id>/bronze/raw_data.parquet
    # run_output_dir = <base>/dagster/<run_id>
    run_output_dir = os.path.dirname(os.path.dirname(bronze_raw_data))
    spark = get_spark_session("dagster_health_pipeline_silver")
    silver_path = run_silver(spark, bronze_raw_data, run_output_dir)
    context.log.info("Silver layer written to: %s", silver_path)
    return silver_path


# ---------------------------------------------------------------------------
# Gold asset – state-level aggregates ready for analytics
# ---------------------------------------------------------------------------

@asset(
    group_name="health_pipeline",
    compute_kind="spark",
    description="Aggregate Silver data by US state → Gold Parquet (record count + mean BMI).",
)
def gold_aggregated_by_state(
    context: OpExecutionContext,
    silver_filtered_data: str,
) -> str:
    """Aggregate Silver Parquet into Gold state-level analytics.

    Accepts the Silver output path from the upstream silver_filtered_data asset.
    The run-scoped output directory is derived from the silver path.

    Returns the full path to the written Parquet directory.
    """
    # silver_path  = <base>/dagster/<run_id>/silver/filtered_data.parquet
    # run_output_dir = <base>/dagster/<run_id>
    run_output_dir = os.path.dirname(os.path.dirname(silver_filtered_data))
    spark = get_spark_session("dagster_health_pipeline_gold")
    gold_path = run_gold(
        spark,
        silver_filtered_data,
        run_output_dir,
        DEFAULT_REF_STATE_CODES_CSV,
    )
    context.log.info("Gold layer written to: %s", gold_path)
    return gold_path
