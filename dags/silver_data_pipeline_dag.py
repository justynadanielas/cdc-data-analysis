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
import tempfile

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType

with DAG(
    dag_id="silver_data_pipeline",
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

    def transform_data(**kwargs):
        # Create a temporary directory for intermediate Parquet files
        # In a production environment, consider a more robust shared storage solution
        # like S3, GCS, HDFS, or a persistent NFS mount.
        # For this example, we'll use a temporary directory within the DAGs folder.
        temp_data_dir = os.path.join(os.path.dirname(__file__), "temp_data")
        os.makedirs(temp_data_dir, exist_ok=True)
        temp_dir = tempfile.mkdtemp(prefix="spark_data_", dir=temp_data_dir)
        output_path = os.path.join(temp_dir, "raw_data.parquet")

        spark = get_spark_session("silver_data_pipeline_generate")
        
        # Read from CSV and select appropriate columns
        csv_path = "/home/justynadanielas/airflow/data/2013.csv"
        df = spark.read.csv(csv_path, header=True, inferSchema=True)
        
        # Select and rename columns to match expected schema
        df = df.select(
            F.col("SEQNO").alias("id"),
            F.col("_STATE").cast(StringType()).alias("category"),
            F.col("HTM4").cast(StringType()).alias("value"),
            F.when(F.col("SMOKE100") == 1.0, "true").otherwise("false").alias("active")
        )

        df.write.mode("overwrite").parquet(output_path)
        print(output_path)
        if "ti" in kwargs:
            kwargs["ti"].xcom_push(key="raw_data_path", value=output_path)
        df.show(truncate=False)

    # def transform_data(**kwargs):
    #     ti = kwargs["ti"]
    #     raw_data_path = ti.xcom_pull(task_ids="generate_data", key="raw_data_path")

    #     # Create a temporary directory for intermediate Parquet files
    #     temp_data_dir = os.path.join(os.path.dirname(__file__), "temp_data")
    #     os.makedirs(temp_data_dir, exist_ok=True)
    #     temp_dir = tempfile.mkdtemp(prefix="spark_data_", dir=temp_data_dir)
    #     output_path = os.path.join(temp_dir, "filtered_data.parquet")

    #     schema = StructType(
    #         [
    #             StructField("id", IntegerType(), nullable=False),
    #             StructField("category", StringType(), nullable=False),
    #             StructField("value", StringType(), nullable=False),
    #             StructField("active", BooleanType(), nullable=False), # Changed to BooleanType for consistency after casting
    #         ]
    #     )

    #     spark = get_spark_session("silver_data_pipeline_transform")
    #     df = spark.read.parquet(raw_data_path)

    #     transformed = (
    #         df.filter((F.col("category").isin(["1.0", "2.0"])) & (F.col("active") == "true"))
    #         .withColumn("value", F.col("value").cast(IntegerType()))
    #         .withColumn("active", F.col("active") == F.lit("true"))
    #     )
        
    #     transformed.write.mode("overwrite").parquet(output_path)
    #     kwargs["ti"].xcom_push(key="filtered_data_path", value=output_path)
    #     transformed.show(truncate=False)

    def aggregate_data(**kwargs):
        ti = kwargs["ti"]
        filtered_data_path = ti.xcom_pull(task_ids="transform_data", key="filtered_data_path")

        spark = get_spark_session("silver_data_pipeline_aggregate")
        df = spark.read.parquet(filtered_data_path)

        aggregated = (
            df.groupBy("category")
            .agg(
                F.count("value").alias("count"),
                F.sum("value").alias("sum"),
                F.mean("value").alias("mean"),
            )
            .orderBy("category")
        )

        aggregated.show(truncate=False)

    generate_task = PythonOperator(
        task_id="generate_data",
        python_callable=generate_data,
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
