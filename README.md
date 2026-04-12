# Health Data Pipeline

Processes the [BRFSS 2013 health survey](https://www.cdc.gov/brfss/) CSV through a **Bronze → Silver → Gold** medallion architecture using **Apache Spark** for processing and **Apache Airflow** for orchestration.

---

## Architecture

```mermaid
flowchart TD
    subgraph inputs["Inputs"]
        CSV["data/2013.csv<br/>(BRFSS 2013 Survey)"]
    end

    subgraph orchestration["Orchestration – Airflow DAG: health_data_pipeline"]
        T1["ingest_raw_data<br/>(Bronze task)"]
        T2["transform_to_silver<br/>(Silver task)"]
        T3["aggregate_to_gold<br/>(Gold task)"]
        T1 --> T2 --> T3
    end

    subgraph spark_module["Spark Module – spark/health_data_pipeline.py"]
        S1["run_bronze()<br/>Read CSV · keep all columns"]
        S2["run_silver()<br/>Filter · label · cast types"]
        S3["run_gold()<br/>Group by state · aggregate BMI"]
    end

    subgraph storage["Outputs – data/pipeline_output/<run_id>/"]
        B[("bronze/<br/>raw_data.parquet")]
        SL[("silver/<br/>filtered_data.parquet")]
        G[("gold/<br/>aggregated_by_state.parquet")]
    end

    CSV --> T1
    T1 -- "delegates to" --> S1
    T2 -- "delegates to" --> S2
    T3 -- "delegates to" --> S3
    S1 -- "writes" --> B
    S2 -- "reads/writes" --> SL
    S3 -- "reads/writes" --> G

    style inputs fill:#f5f5f5,stroke:#ccc
    style orchestration fill:#e8f4fd,stroke:#5b9bd5
    style spark_module fill:#fff7e6,stroke:#f0a500
    style storage fill:#e8fce8,stroke:#4caf50
```

---

## Pipeline Layers

| Layer  | Task               | Spark function   | Output path                                    | What happens                                       |
|--------|--------------------|------------------|------------------------------------------------|----------------------------------------------------|
| Bronze | `ingest_raw_data`  | `run_bronze()`   | `bronze/raw_data.parquet`                      | Raw CSV ingested as-is; no schema changes          |
| Silver | `transform_to_silver` | `run_silver()` | `silver/filtered_data.parquet`               | Invalid rows dropped; columns cleaned and labelled |
| Gold   | `aggregate_to_gold` | `run_gold()`    | `gold/aggregated_by_state.parquet`             | BMI aggregated by US state for analytics           |

Each DAG run writes its output under `data/pipeline_output/<run_id>/` so runs are isolated and **idempotent** — re-running the same DAG run safely overwrites its own output without touching other runs.

---

## Repository Structure

```
airflow/
├── dags/
│   └── health_data_pipeline_dag.py   # Airflow DAG – thin orchestration only
├── spark/
│   ├── __init__.py
│   └── health_data_pipeline.py       # Spark pipeline – single entry point
├── data/
│   └── 2013.csv                      # Source data (BRFSS 2013)
├── pipeline_flow.md                  # Mermaid diagram source
└── README.md                         # This file
```

---

## Running the Pipeline

### Standalone (no Airflow required)

```bash
# Run with defaults (reads data/2013.csv, writes to data/pipeline_output/)
python spark/health_data_pipeline.py

# Run with custom paths
python spark/health_data_pipeline.py \
  --input data/2013.csv \
  --output-dir data/pipeline_output
```

### Via Airflow

```bash
# Start the Airflow server
airflow standalone

# Trigger the DAG
airflow dags trigger health_data_pipeline
```

---

## Silver Layer – Transformations

| Source column | Output column            | Transformation                                  |
|---------------|--------------------------|-------------------------------------------------|
| `_STATE`      | `State_Code`             | Kept as-is                                      |
| `GENHLTH`     | `General_Health`         | 1–5 mapped to Excellent/Very Good/Good/Fair/Poor; other values dropped |
| `_BMI5`       | `BMI_Value`              | Divided by 100 (stored as ×100 int); null/zero rows dropped |
| `_TOTINDA`    | `Physical_Activity_Status` | 1 → Active, 2 → Inactive, other → Unknown     |
| `_AGEG5YR`    | `Age_Group_Code`         | Kept as-is                                      |
| `_RACE`       | `Race_Code`              | Kept as-is                                      |
