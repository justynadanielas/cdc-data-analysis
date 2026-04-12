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
### Silver Data Pipeline DAG
Generate sample data, filter rows, convert types, and aggregate results.
"""

from __future__ import annotations

import pendulum
import os
import subprocess
import sys
import tempfile

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType

with DAG(
    dag_id="health_data_pipeline",
    default_args={"retries": 1},
    description="Generate data, filter, convert types, and aggregate using Airflow tasks.",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "silver"],
) as dag:
    dag.doc_md = __doc__

    def get_spark_session(app_name: str) -> SparkSession:
        return SparkSession.builder.appName(app_name).getOrCreate()

    def load_data(**kwargs):
        # Create a temporary directory for intermediate Parquet files
        # In a production environment, consider a more robust shared storage solution
        # like S3, GCS, HDFS, or a persistent NFS mount.
        # For this example, we'll use a temporary directory within the DAGs folder.
        temp_data_dir = os.path.join(os.path.dirname(__file__), "temp_data")
        os.makedirs(temp_data_dir, exist_ok=True)
        temp_dir = tempfile.mkdtemp(prefix="spark_data_", dir=temp_data_dir)
        output_path = os.path.join(temp_dir, "raw_data.parquet")

        spark = get_spark_session("health_data_pipeline_generate")
        
        # Read from CSV - keep all columns for downstream processing
        csv_path = "/home/justynadanielas/airflow/data/2013.csv"
        df = spark.read.csv(csv_path, header=True, inferSchema=True)

        df.write.mode("overwrite").parquet(output_path)
        print(f"Raw data saved to: {output_path}")
        print(f"Total records: {df.count()}")
        if "ti" in kwargs:
            kwargs["ti"].xcom_push(key="raw_data_path", value=output_path)
        df.show(10, truncate=False)

    def transform_data(**kwargs):
        ti = kwargs["ti"]
        raw_data_path = ti.xcom_pull(task_ids="generate_data", key="raw_data_path")

        # Create a temporary directory for intermediate Parquet files
        temp_data_dir = os.path.join(os.path.dirname(__file__), "temp_data")
        os.makedirs(temp_data_dir, exist_ok=True)
        temp_dir = tempfile.mkdtemp(prefix="spark_data_", dir=temp_data_dir)
        output_path = os.path.join(temp_dir, "filtered_data.parquet")

        spark = get_spark_session("health_data_pipeline_transform")
        
        # Read the raw data from generate_data
        df = spark.read.parquet(raw_data_path)
        
        # Apply transformations equivalent to the SQL
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
                F.col("_RACE").alias("Race_Code")
            )
            .filter(
                (F.col("GENHLTH") <= 5) &
                (F.col("_BMI5").isNotNull()) &
                (F.col("_BMI5") > 0)
            )
        )
        
        transformed.write.mode("overwrite").parquet(output_path)
        kwargs["ti"].xcom_push(key="filtered_data_path", value=output_path)
        transformed.show(truncate=False)

    def aggregate_data(**kwargs):
        ti = kwargs["ti"]
        filtered_data_path = ti.xcom_pull(task_ids="transform_data", key="filtered_data_path")

        spark = get_spark_session("health_data_pipeline_aggregate")
        df = spark.read.parquet(filtered_data_path)

        aggregated = (
            df.groupBy("State_Code")
            .agg(
                F.count("BMI_Value").alias("count"),
                F.sum("BMI_Value").alias("sum"),
                F.mean("BMI_Value").alias("mean"),
            )
            .orderBy("State_Code")
        )
        aggregated.show(truncate=False)

    generate_task = PythonOperator(
        task_id="generate_data",
        python_callable=load_data,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    aggregate_task = PythonOperator(
        task_id="aggregate_data",
        python_callable=aggregate_data,
    )

    generate_task >> transform_task >> aggregate_task


def trigger_dag(dag_id: str = "health_data_pipeline") -> None:
    """Trigger the Airflow DAG using the current Python environment's Airflow CLI."""
    cmd = [sys.executable, "-m", "airflow", "dags", "trigger", dag_id]
    print(f"Triggering DAG '{dag_id}' using: {cmd}")
    subprocess.run(cmd, check=True)
    print(f"DAG '{dag_id}' triggered successfully.")


if __name__ == "__main__":
    trigger_dag()
