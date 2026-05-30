#!/usr/bin/env python
"""run_parity.py – Single-command reproducible parity flow.

Runs three steps in sequence and exits with the comparison result code:

  Step 1 (airflow-baseline)  Run the Spark pipeline standalone – same logic
                             as the Airflow DAG – and write run_manifest.json
                             + latest_airflow_run.json so compare_parity.py
                             can pick it up.

  Step 2 (dagster)           Execute health_data_pipeline_job in-process.
                             The job writes its own manifest and updates
                             data/pipeline_output/dagster/latest_run.json.

  Step 3 (compare)           Run compare_parity.py deterministic checks.
                             Exits 0=PASS, 1=FAIL.

Usage
-----
    python run_parity.py                   # full 3-step flow
    python run_parity.py --skip-airflow    # reuse existing latest_airflow_run.json
    python run_parity.py --skip-dagster    # reuse existing dagster/latest_run.json
    python run_parity.py --compare-only    # skip both pipeline steps

Exit codes
----------
    0  all parity checks passed
    1  one or more parity checks failed
    2  a pipeline step itself failed (check stderr for details)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Project root on path so local modules are importable
# ---------------------------------------------------------------------------
_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


# ---------------------------------------------------------------------------
# Step 1 – Airflow-equivalent standalone Spark run
# ---------------------------------------------------------------------------

def _step_airflow_baseline() -> None:
    """Run the Spark pipeline standalone and write the Airflow-format manifest."""
    from spark.health_data_pipeline import (
        DEFAULT_INPUT_CSV,
        DEFAULT_OUTPUT_DIR,
        DEFAULT_REF_STATE_CODES_CSV,
        run_pipeline,
    )

    run_id = "standalone_" + datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    output_dir = os.path.join(DEFAULT_OUTPUT_DIR, run_id)
    os.makedirs(output_dir, exist_ok=True)

    print(f"\n[Step 1/3] Airflow-equivalent Spark run  (run_id={run_id})")
    print(f"           input  : {DEFAULT_INPUT_CSV}")
    print(f"           output : {output_dir}")
    t0 = time.time()
    paths = run_pipeline(DEFAULT_INPUT_CSV, output_dir, DEFAULT_REF_STATE_CODES_CSV)
    elapsed = round(time.time() - t0, 3)
    print(f"           done in {elapsed}s")

    now = datetime.now(timezone.utc).isoformat()
    manifest = {
        "orchestrator": "airflow",
        "run_id": run_id,
        "captured_at": now,
        "input_file": DEFAULT_INPUT_CSV,
        "output_dir": output_dir,
        "output_paths": paths,
        "wall_clock_seconds": elapsed,
    }

    manifest_path = os.path.join(output_dir, "run_manifest.json")
    with open(manifest_path, "w") as fh:
        json.dump(manifest, fh, indent=2)

    latest_pointer = os.path.join(DEFAULT_OUTPUT_DIR, "latest_airflow_run.json")
    with open(latest_pointer, "w") as fh:
        json.dump(
            {"run_id": run_id, "manifest_path": manifest_path, "captured_at": now},
            fh,
            indent=2,
        )
    print(f"           manifest  → {manifest_path}")
    print(f"           pointer   → {latest_pointer}")


# ---------------------------------------------------------------------------
# Step 2 – Dagster direct job
# ---------------------------------------------------------------------------

def _step_dagster() -> None:
    """Execute health_data_pipeline_job in-process (headless Dagster run)."""
    from dagster_pipeline.definitions import defs

    print("\n[Step 2/3] Dagster direct job  (health_data_pipeline_job)")
    t0 = time.time()
    result = defs.get_job_def("health_data_pipeline_job").execute_in_process()
    elapsed = round(time.time() - t0, 3)

    if not result.success:
        raise RuntimeError(
            "Dagster job health_data_pipeline_job failed – "
            "check the events above for details."
        )

    print(f"           done in {elapsed}s  run_id={result.run_id}")


# ---------------------------------------------------------------------------
# Step 3 – Parity comparison
# ---------------------------------------------------------------------------

def _step_compare(report_path: str, bmi_tolerance: float) -> int:
    """Run deterministic parity checks. Returns 0=PASS, 1=FAIL."""
    from compare_parity import run_comparison

    print(f"\n[Step 3/3] Parity comparison  (report → {report_path})")
    t0 = time.time()
    exit_code = run_comparison(
        airflow_manifest_path=None,  # reads latest_airflow_run.json pointer
        dagster_manifest_path=None,  # reads dagster/latest_run.json pointer
        bmi_tolerance=bmi_tolerance,
        report_path=report_path,
    )
    elapsed = round(time.time() - t0, 3)
    status = "PASS" if exit_code == 0 else "FAIL"
    print(f"\n           result: {status}  ({elapsed}s)")
    return exit_code


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--skip-airflow",
        action="store_true",
        help="Skip step 1 – reuse the existing latest_airflow_run.json baseline.",
    )
    p.add_argument(
        "--skip-dagster",
        action="store_true",
        help="Skip step 2 – reuse the existing dagster/latest_run.json output.",
    )
    p.add_argument(
        "--compare-only",
        action="store_true",
        help="Skip steps 1 and 2 – only run the parity comparison.",
    )
    p.add_argument(
        "--bmi-tolerance",
        type=float,
        default=1e-6,
        metavar="FLOAT",
        help="Max absolute bmi_mean difference allowed (default: 1e-6).",
    )
    p.add_argument(
        "--report-path",
        default=os.path.join(
            _PROJECT_ROOT, "data", "pipeline_output", "parity_report.json"
        ),
        metavar="PATH",
        help="Output path for the JSON parity report.",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    skip_airflow = args.skip_airflow or args.compare_only
    skip_dagster = args.skip_dagster or args.compare_only

    wall_start = time.time()
    print("=" * 62)
    print("  Parity flow: Airflow-baseline → Dagster → compare")
    print("=" * 62)
    if skip_airflow:
        print("[Step 1/3] skipped – reusing existing Airflow baseline")
    if skip_dagster:
        print("[Step 2/3] skipped – reusing existing Dagster output")

    try:
        if not skip_airflow:
            _step_airflow_baseline()
        if not skip_dagster:
            _step_dagster()
    except Exception as exc:
        print(f"\n[ERROR] Pipeline step failed: {exc}", file=sys.stderr)
        sys.exit(2)

    exit_code = _step_compare(args.report_path, args.bmi_tolerance)

    total = round(time.time() - wall_start, 1)
    print(f"\nTotal wall time: {total}s")
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
