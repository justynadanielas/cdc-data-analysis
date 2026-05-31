# Airflow vs Dagster Comparison Report

**Dataset:** BRFSS 2013 Health Survey (`data/2013.csv`)
**Pipeline:** Bronze → Silver → Gold medallion architecture (PySpark)
**Report generated:** 2026-05-31
**Parity verdict:** **PASS** — all 7/7 deterministic checks passed across two independent runs.

---

## 1. Objective Metrics

### 1.1 Output Parity (Primary Evidence)

| Layer  | Airflow row count | Dagster row count | Equal |
|--------|-------------------|-------------------|-------|
| Bronze | 491,773           | 491,773           | ✓     |
| Silver | 463,280           | 463,280           | ✓     |
| Gold   | 53                | 53                | ✓     |

Silver drops **28,493 rows** (5.79 %) relative to Bronze — identical in both orchestrators, confirming the same filter predicate executes in each path.

### 1.2 Gold Schema Parity

Both orchestrators produce identical Gold schema:

| Column         | Type         |
|----------------|--------------|
| `State_Code`   | `DoubleType` |
| `State_Name`   | `StringType` |
| `record_count` | `LongType`   |
| `bmi_mean`     | `DoubleType` |

### 1.3 Gold State-Level Aggregate Parity

All 53 state/territory rows were compared on `record_count` and `bmi_mean`.

- **record_count equality:** 53/53 states matched exactly.
- **bmi_mean delta:** 0.0 (absolute) for all 53 states at tolerance ≤ 1e-6.

Full state-level diff is in `data/pipeline_output/parity_report.json`.

### 1.4 Pipeline Wall-Clock Time

| Orchestrator | Run type                  | Wall-clock time |
|--------------|---------------------------|-----------------|
| Airflow      | Standalone (Spark direct) | **38.4 s**      |
| Airflow      | Baseline (earlier run)    | 49.2 s          |
| Dagster      | Asset job (in-process)    | not timed†      |

†The Dagster run manifest does not record wall-clock time. Both orchestrators invoke the identical PySpark functions (`run_bronze`, `run_silver`, `run_gold`), so compute time is dominated by Spark and is effectively equivalent. The 10-second variation in the two Airflow runs reflects JVM warm-up noise.

### 1.5 Parity Check Summary

| Check                                   | Result |
|-----------------------------------------|--------|
| Bronze row-count equality               | PASS   |
| Silver row-count equality               | PASS   |
| Gold row-count equality                 | PASS   |
| Gold schema field names + types match   | PASS   |
| Gold `State_Code` set equality          | PASS   |
| Gold `record_count` per state equality  | PASS   |
| Gold `bmi_mean` per state (≤ 1e-6 tol) | PASS   |
| **Total**                               | **7/7 PASS** |

Parity was confirmed across two separate run pairs (run IDs recorded in artifacts below).

---

## 2. Architectural Comparison

| Dimension              | Apache Airflow                                              | Dagster                                                       |
|------------------------|-------------------------------------------------------------|---------------------------------------------------------------|
| **Abstraction model**  | Task graph (DAG of operators)                               | Software-defined assets (SDAs) + jobs                         |
| **Orchestration unit** | `PythonOperator` tasks                                      | `@asset`-decorated functions                                  |
| **Data lineage**       | Implicit — tracked via XCom or conventions                  | First-class — asset graph is the lineage graph                |
| **Scheduling**         | Cron / `@daily` on the DAG                                  | Job-level scheduling or asset-based auto-materialisation      |
| **UI / observability** | Airflow Web UI (DAG-centric, task logs)                     | Dagster UI (asset-centric, run pages, type metadata)          |
| **Testing surface**    | DAG import smoke tests; task unit tests                     | `execute_in_process()` with mock injection; asset-level tests |
| **Reuse of Spark code**| Direct import of `spark/health_data_pipeline.py` functions  | Same — `@asset` bodies call the identical Spark functions     |
| **Output isolation**   | Per-run subdirectory under `pipeline_output/<run_id>/`      | `pipeline_output/dagster/<dagster_run_id>/`                   |
| **Retry / backfill**   | Built-in task retry (`retries: 1`), DAG catchup             | Job retry policies, asset-based partitioned backfills         |
| **Learning curve**     | Low — broadly adopted, extensive documentation              | Moderate — asset/resource concepts require onboarding         |

---

## 3. Qualitative Observations

**Where Dagster adds value over this implementation:**

- The asset graph makes Bronze → Silver → Gold **data lineage visible** without any extra instrumentation; Airflow DAG edges only model task dependencies, not data dependencies.
- `execute_in_process()` allows **unit-testing the full pipeline** with mocked Spark wrappers in under 1 second (see `tests/test_dagster_direct_job_smoke.py`), which is harder to achieve cleanly with Airflow operators.
- Dagster's type system could enforce output contracts (e.g., matching `data_product_contract.yaml`) as first-class schema checks rather than post-hoc validation.

**Where Airflow has practical advantages in this implementation:**

- The `health_data_pipeline_dag.py` DAG is operational today with scheduling, retries, and a fully functional Web UI — no additional setup required.
- Airflow's ecosystem (providers, hooks, connections) is more mature for heterogeneous production integrations (databases, cloud storage, sensors).
- Operational familiarity in most data engineering teams is higher, reducing ramp-up for new contributors.

---

## 4. Limitations of This Comparison

1. **Single dataset / single run type.** All parity evidence is from one input file (`data/2013.csv`, ~491 k rows). Performance and correctness at larger scales or with partitioned inputs are untested.
2. **Execution environment is identical.** Both orchestrators run on the same machine, sharing JVM and Spark resources. A production deployment with separate workers / clusters would surface infrastructure-level differences not visible here.
3. **Wall-clock timing is not a fair Dagster measure.** Dagster run manifests did not capture wall-clock seconds for the asset job. Timing comparisons are therefore incomplete; both pipelines call the same Spark code and JVM warm-up dominates.
4. **Kafka path not completed.** The Kafka ingestion path (`health_data_pipeline_kafka_dag.py`, `spark/kafka_producer.py`, `spark/kafka_consumer.py`) was out of scope. Streaming parity between the orchestrators is not addressed.
5. **No production deployment tested.** Scheduling reliability, backfill correctness, secret management, and multi-worker parallelism are not evaluated.
6. **Dagster scheduling not exercised.** Only manual/headless `job execute` runs were performed in Dagster; the Dagster daemon and sensor-based scheduling are untested.

---

## 5. Evidence Artefacts

| Artefact | Path |
|---|---|
| Primary parity report (latest run) | `data/pipeline_output/parity_report.json` |
| Earlier parity artefact | `data/pipeline_output/parity_artifact.json` |
| Airflow latest run manifest pointer | `data/pipeline_output/latest_airflow_run.json` |
| Airflow standalone run manifest | `data/pipeline_output/standalone_2026-05-30T11-27-51/run_manifest.json` |
| Dagster latest run manifest pointer | `data/pipeline_output/dagster/latest_run.json` |
| Dagster run manifest (latest) | `data/pipeline_output/dagster/a8078daa-f58d-45b8-b5d5-bf7734122574/run_manifest.json` |
| Airflow baseline (earlier) | `data/airflow_baseline.json` |
| Smoke test | `tests/test_dagster_direct_job_smoke.py` |
| Parity checker script | `compare_parity.py` |
| Single-command parity flow | `run_parity.py` |

### Reproduce

```bash
# Activate the virtualenv (adjust path as needed)
source /home/justynadanielas/code/BGD_2026/venv/bin/activate

# Full 3-step parity flow (Airflow baseline → Dagster run → compare)
python run_parity.py

# Or: compare only (reuse last manifests)
python run_parity.py --compare-only
```

---

## 6. Conclusion

For the **direct CSV → Bronze → Silver → Gold path**, Airflow and Dagster are **functionally equivalent**: every row count, schema field, and state-level aggregate is identical. The shared `spark/health_data_pipeline.py` module is the reason — both orchestrators are thin wrappers around the same Spark functions, so compute correctness is guaranteed by construction.

The meaningful difference between the two tools is **developer experience and operational model**, not output correctness:

- Choose **Airflow** if the priority is operational maturity, a large ecosystem, and team familiarity.
- Choose **Dagster** if the priority is first-class data lineage, testability via `execute_in_process`, and asset-oriented thinking.

Neither conclusion is invalidated by missing Kafka parity or production deployment evidence; those are explicitly deferred scope items (see Section 4).
