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

    def generate_data(**kwargs):
        rows = [
            {
                "id": i,
                "category": "A" if i % 3 == 0 else "B" if i % 3 == 1 else "C",
                "value": str(i * 10),
                "active": "true" if i % 2 == 0 else "false",
            }
            for i in range(1, 11)
        ]

        schema = StructType(
            [
                StructField("id", IntegerType(), nullable=False),
                StructField("category", StringType(), nullable=False),
                StructField("value", StringType(), nullable=False),
                StructField("active", StringType(), nullable=False),
            ]
        )

        spark = get_spark_session("silver_data_pipeline_generate")
        df = spark.createDataFrame(rows, schema=schema)
        kwargs["ti"].xcom_push(key="raw_data", value=[row.asDict() for row in df.collect()])
        df.show(truncate=False)

    def transform_data(**kwargs):
        ti = kwargs["ti"]
        raw_data = ti.xcom_pull(task_ids="generate_data", key="raw_data")

        schema = StructType(
            [
                StructField("id", IntegerType(), nullable=False),
                StructField("category", StringType(), nullable=False),
                StructField("value", StringType(), nullable=False),
                StructField("active", StringType(), nullable=False),
            ]
        )

        spark = get_spark_session("silver_data_pipeline_transform")
        df = spark.createDataFrame(raw_data, schema=schema)

        transformed = (
            df.filter((F.col("category").isin(["A", "B"])) & (F.col("active") == "true"))
            .withColumn("value", F.col("value").cast(IntegerType()))
            .withColumn("active", F.col("active") == F.lit("true"))
        )

        ti.xcom_push(key="filtered_data", value=[row.asDict() for row in transformed.collect()])
        transformed.show(truncate=False)

    def aggregate_data(**kwargs):
        ti = kwargs["ti"]
        filtered_data = ti.xcom_pull(task_ids="transform_data", key="filtered_data")

        spark = get_spark_session("silver_data_pipeline_aggregate")
        df = spark.createDataFrame(filtered_data)

        aggregated = (
            df.groupBy("category")
            .agg(
                F.count("value").alias("count"),
                F.sum("value").alias("sum"),
                F.mean("value").alias("mean"),
            )
            .orderBy("category")
        )

        ti.xcom_push(key="aggregated_data", value=[row.asDict() for row in aggregated.collect()])
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
