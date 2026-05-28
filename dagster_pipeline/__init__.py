"""Dagster pipeline package – Bronze → Silver → Gold health data pipeline.

Wraps the existing Spark logic in spark/health_data_pipeline.py so the same
transformations run under Dagster for a like-for-like parity comparison with
the Airflow DAG in dags/health_data_pipeline_dag.py.
"""
