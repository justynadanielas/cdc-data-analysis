```mermaid
flowchart TD
    subgraph inputs["Inputs"]
        CSV["data/2013.csv<br/>(BRFSS 2013 Survey)"]
    end

    subgraph kafka_stack["Kafka Stack (docker compose)"]
        BROKER["Kafka Broker<br/>localhost:9092<br/>(KRaft – no ZooKeeper)"]
        TOPIC[("Topic:<br/>health-data-raw")]
        BROKER --- TOPIC
    end

    subgraph dag_direct["Airflow DAG: health_data_pipeline"]
        T1["ingest_raw_data<br/>(Bronze)"]
        T2_d["transform_to_silver"]
        T3_d["aggregate_to_gold"]
        T1 --> T2_d --> T3_d
    end

    subgraph dag_kafka["Airflow DAG: health_data_pipeline_kafka"]
        P["produce_to_kafka<br/>(max 5,000 records)"]
        C["consume_from_kafka_to_bronze<br/>(unique group per run_id)"]
        T2_k["transform_to_silver"]
        T3_k["aggregate_to_gold"]
        P --> C --> T2_k --> T3_k
    end

    subgraph spark_core["spark/health_data_pipeline.py"]
        S1["run_bronze()<br/>Read CSV · keep all columns"]
        S2["run_silver()<br/>Filter · label · cast types"]
        S3["run_gold()<br/>Group by state · aggregate BMI"]
    end

    subgraph producer_module["spark/kafka_producer.py"]
        PROD["produce_csv_to_kafka()<br/>pandas chunks → JSON messages"]
    end

    subgraph consumer_module["spark/kafka_consumer.py"]
        CONS["consume_kafka_to_bronze()<br/>KafkaConsumer → JSONL → Parquet"]
    end

    subgraph storage["Outputs – data/pipeline_output/&lt;run_id&gt;/"]
        B[("bronze/<br/>raw_data.parquet")]
        SL[("silver/<br/>filtered_data.parquet")]
        G[("gold/<br/>aggregated_by_state.parquet")]
    end

    %% Direct path
    CSV --> T1
    T1 -- "delegates to" --> S1
    T2_d -- "delegates to" --> S2
    T3_d -- "delegates to" --> S3

    %% Kafka path
    CSV --> P
    P -- "delegates to" --> PROD
    PROD -- "publish JSON rows" --> BROKER
    C -- "delegates to" --> CONS
    CONS -- "poll messages" --> TOPIC

    %% Shared Silver/Gold (both DAGs)
    T2_k -- "delegates to" --> S2
    T3_k -- "delegates to" --> S3

    %% Writes
    S1 -- "writes" --> B
    CONS -- "writes" --> B
    S2 -- "reads/writes" --> SL
    S3 -- "reads/writes" --> G

    style inputs fill:#f5f5f5,stroke:#ccc
    style kafka_stack fill:#fff0f0,stroke:#e53935
    style dag_direct fill:#e8f4fd,stroke:#5b9bd5
    style dag_kafka fill:#e8f4fd,stroke:#5b9bd5
    style spark_core fill:#fff7e6,stroke:#f0a500
    style producer_module fill:#fce8ff,stroke:#9c27b0
    style consumer_module fill:#fce8ff,stroke:#9c27b0
    style storage fill:#e8fce8,stroke:#4caf50
```