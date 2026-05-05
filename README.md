# Health Data Pipeline

Processes the [BRFSS 2013 health survey](https://www.cdc.gov/brfss/) CSV through a **Bronze → Silver → Gold** medallion architecture using **Apache Spark** for processing and **Apache Airflow** for orchestration.

---

## Prerequisites

Libraries defined in `requirements.txt` + **Java 17** + **Docker** (for the Kafka stack)

### Setup

```bash
# 1. Clone the repo and enter its directory
git clone <repo-url>

# 2. Create and activate a virtual environment
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate

# 3. Install Python dependencies
pip install -r requirements.txt

# 4. Download the source data (see "Source Data" section below)

# 5. (Optional) Start the local Kafka stack
docker compose up -d
```

---

## Source Data

The pipeline expects the **BRFSS 2013 Behavioral Risk Factor Surveillance System** dataset at `data/2013.csv`.

1. Go to the Kaggle dataset page:
   **<https://www.kaggle.com/datasets/cdc/behavioral-risk-factor-surveillance-system?select=2013.csv>**
2. Click **Download** (requires a free Kaggle account) and select `2013.csv`, or download the full dataset and extract `2013.csv`.
3. Place the file at `data/2013.csv` inside the project root.

> The file is ~1 GB. `data/` is gitignored so it is never committed.

---

## Architecture

Two ingestion paths share the same Silver and Gold processing logic.

### Path A — Direct CSV (original)

```mermaid
flowchart TD
    subgraph inputs["Inputs"]
        CSV["data/2013.csv<br/>(BRFSS 2013 Survey)"]
    end

    subgraph orchestration["Airflow DAG: health_data_pipeline"]
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

### Path B — Kafka Queue (new)

```mermaid
flowchart TD
    subgraph inputs["Inputs"]
        CSV["data/2013.csv<br/>(BRFSS 2013 Survey)"]
    end

    subgraph kafka_stack["Kafka Stack (docker compose)"]
        BROKER["Kafka Broker<br/>localhost:9092<br/>(KRaft)"]
        TOPIC[("Topic:<br/>health-data-raw")]
        UI["Kafka UI<br/>localhost:8081"]
        BROKER --- TOPIC
        BROKER --- UI
    end

    subgraph orchestration["Airflow DAG: health_data_pipeline_kafka"]
        FS["watch_for_data<br/>(FileSensor)"]
        P["produce_to_kafka"]
        C["consume_from_kafka_to_bronze"]
        T2["transform_to_silver"]
        T3["aggregate_to_gold"]
        FS --> P --> C --> T2 --> T3
    end

    subgraph producer_module["spark/kafka_producer.py"]
        PROD["produce_csv_to_kafka()<br/>pandas chunks → JSON messages"]
    end

    subgraph spark_module["spark/health_data_pipeline.py"]
        S1K["run_bronze_from_kafka()<br/>Consume Kafka → Parquet"]
        S2["run_silver()"]
        S3["run_gold()"]
    end

    subgraph storage["Outputs – data/pipeline_output/<run_id>/"]
        B[("bronze/<br/>raw_data.parquet")]
        SL[("silver/<br/>filtered_data.parquet")]
        G[("gold/<br/>aggregated_by_state.parquet")]
    end

    CSV -- "file present?" --> FS
    P -- "delegates to" --> PROD
    PROD -- "publish JSON rows" --> BROKER
    C -- "delegates to" --> S1K
    S1K -- "consume from" --> TOPIC
    T2 -- "delegates to" --> S2
    T3 -- "delegates to" --> S3
    S1K -- "writes" --> B
    S2 -- "reads/writes" --> SL
    S3 -- "reads/writes" --> G

    style inputs fill:#f5f5f5,stroke:#ccc
    style kafka_stack fill:#fff0f0,stroke:#e53935
    style orchestration fill:#e8f4fd,stroke:#5b9bd5
    style producer_module fill:#fce8ff,stroke:#9c27b0
    style spark_module fill:#fff7e6,stroke:#f0a500
    style storage fill:#e8fce8,stroke:#4caf50
```

---

## Pipeline Layers

| Layer  | Task                          | Spark function            | Output path                        | What happens                                       |
|--------|-------------------------------|---------------------------|------------------------------------|----------------------------------------------------|
| Bronze | `ingest_raw_data`             | `run_bronze()`            | `bronze/raw_data.parquet`          | Raw CSV ingested as-is; no schema changes          |
| Bronze | `consume_from_kafka_to_bronze`| `run_bronze_from_kafka()` | `bronze/raw_data.parquet`          | Kafka messages consumed and written as Parquet     |
| Silver | `transform_to_silver`         | `run_silver()`            | `silver/filtered_data.parquet`     | Invalid rows dropped; columns cleaned and labelled |
| Gold   | `aggregate_to_gold`           | `run_gold()`              | `gold/aggregated_by_state.parquet` | BMI aggregated by US state for analytics           |

Each DAG run writes its output under `data/pipeline_output/<run_id>/` so runs are isolated and **idempotent** — re-running the same DAG run safely overwrites its own output without touching other runs.

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

### Via Airflow – Direct CSV path (original DAG)

```bash
# Start the Airflow server
airflow standalone

# Trigger the DAG
airflow dags trigger health_data_pipeline
```

### Via Airflow – Kafka path (new DAG)

```bash
# 1. Start local Kafka (KRaft mode, no Zookeeper)
docker compose up -d

# Kafka UI is available at http://localhost:8081

# 2. Start the Airflow server (if not already running)
airflow standalone

# 3. Trigger the Kafka DAG
airflow dags trigger health_data_pipeline_kafka
```
