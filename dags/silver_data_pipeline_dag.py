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

import pandas as pd
import pendulum

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

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
        df = pd.DataFrame(rows)
        kwargs["ti"].xcom_push(key="raw_data", value=df.to_dict(orient="records"))
        print("Generated data:\n", df)

    def transform_data(**kwargs):
        ti = kwargs["ti"]
        raw_data = ti.xcom_pull(task_ids="generate_data", key="raw_data")
        df = pd.DataFrame(raw_data)

        filtered = df[(df["category"].isin(["A", "B"])) & (df["active"] == "true")].copy()
        filtered["value"] = filtered["value"].astype(int)
        filtered["active"] = filtered["active"].map({"true": True, "false": False})

        ti.xcom_push(key="filtered_data", value=filtered.to_dict(orient="records"))
        print("Filtered and converted data:\n", filtered)

    def aggregate_data(**kwargs):
        ti = kwargs["ti"]
        filtered_data = ti.xcom_pull(task_ids="transform_data", key="filtered_data")
        df = pd.DataFrame(filtered_data)

        aggregated = df.groupby("category")["value"].agg(["count", "sum", "mean"]).reset_index()
        ti.xcom_push(key="aggregated_data", value=aggregated.to_dict(orient="records"))
        print("Aggregated results:\n", aggregated)

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
