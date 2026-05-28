"""Dagster Definitions – registers all assets and jobs for this repository.

Run with:
    dagster dev -f dagster_pipeline/definitions.py

Or via workspace.yaml (recommended for multi-module projects):
    dagster dev                # picks up workspace.yaml from project root
    dagster job execute -j health_data_pipeline_job   # headless run
"""

from dagster import AssetSelection, Definitions, define_asset_job

from dagster_pipeline.assets import (
    bronze_raw_data,
    gold_aggregated_by_state,
    silver_filtered_data,
)

# ---------------------------------------------------------------------------
# Job – materialize the full Bronze → Silver → Gold chain in one execution
# ---------------------------------------------------------------------------

health_data_pipeline_job = define_asset_job(
    name="health_data_pipeline_job",
    selection=AssetSelection.all(),
    description=(
        "Materialize Bronze → Silver → Gold health data assets "
        "(Dagster parity run for comparison with Airflow DAG)."
    ),
)

# ---------------------------------------------------------------------------
# Top-level Definitions object – Dagster's single source of truth for this
# code location.  All assets and jobs must be registered here.
# ---------------------------------------------------------------------------

defs = Definitions(
    assets=[
        bronze_raw_data,
        silver_filtered_data,
        gold_aggregated_by_state,
    ],
    jobs=[health_data_pipeline_job],
)
