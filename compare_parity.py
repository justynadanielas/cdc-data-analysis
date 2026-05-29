"""Deterministic Airflow vs Dagster parity checker.

Reads the latest run manifest from each orchestrator, loads the Bronze, Silver,
and Gold Parquet outputs, and executes a fixed set of deterministic checks:

  1. Bronze row-count equality
  2. Silver row-count equality
  3. Gold row-count equality
  4. Gold schema field names + types match
  5. Gold State_Code set equality (no missing / extra states)
  6. Gold record_count per state equality
  7. Gold bmi_mean per state equality (within tolerance)

Writes a JSON parity report to ``data/pipeline_output/parity_report.json`` and
exits with code 0 if all checks pass, 1 if any fail.

Usage
-----
    python compare_parity.py [--airflow-manifest PATH] [--dagster-manifest PATH]
                             [--bmi-tolerance 0.0001] [--report-path PATH]
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Default manifest locations
# ---------------------------------------------------------------------------
_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_AIRFLOW_POINTER = os.path.join(
    _PROJECT_ROOT, "data", "pipeline_output", "latest_airflow_run.json"
)
_DEFAULT_DAGSTER_POINTER = os.path.join(
    _PROJECT_ROOT, "data", "pipeline_output", "dagster", "latest_run.json"
)
_DEFAULT_REPORT_PATH = os.path.join(
    _PROJECT_ROOT, "data", "pipeline_output", "parity_report.json"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_manifest(pointer_path: str) -> dict[str, Any]:
    """Follow a pointer file → actual manifest dict."""
    with open(pointer_path) as fh:
        pointer = json.load(fh)
    manifest_path = pointer.get("manifest_path")
    if not manifest_path:
        raise ValueError(f"No 'manifest_path' key in pointer file: {pointer_path}")
    with open(manifest_path) as fh:
        return json.load(fh)


def _load_manifest_direct(path: str) -> dict[str, Any]:
    """Load a manifest JSON directly."""
    with open(path) as fh:
        return json.load(fh)


def _read_parquet_count(spark, path: str) -> int:
    return spark.read.parquet(path).count()


def _read_parquet_schema(spark, path: str) -> dict[str, str]:
    """Return {field_name: type_string} for a Parquet file."""
    schema = spark.read.parquet(path).schema
    return {f.name: str(f.dataType) for f in schema.fields}


def _read_gold_rows(spark, path: str) -> list[dict[str, Any]]:
    """Return gold rows as a list of dicts sorted by State_Code."""
    rows = spark.read.parquet(path).orderBy("State_Code").collect()
    result = []
    for row in rows:
        result.append({
            "State_Code": row["State_Code"],
            "State_Name": row["State_Name"],
            "record_count": int(row["record_count"]),
            "bmi_mean": float(row["bmi_mean"]) if row["bmi_mean"] is not None else None,
        })
    return result


# ---------------------------------------------------------------------------
# Check helpers
# ---------------------------------------------------------------------------

class Check:
    def __init__(self, name: str):
        self.name = name
        self.passed: bool = False
        self.detail: str = ""

    def ok(self, detail: str = "") -> "Check":
        self.passed = True
        self.detail = detail
        return self

    def fail(self, detail: str) -> "Check":
        self.passed = False
        self.detail = detail
        return self

    def to_dict(self) -> dict[str, Any]:
        return {
            "check": self.name,
            "passed": self.passed,
            "detail": self.detail,
        }


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def check_row_count(layer: str, airflow_count: int, dagster_count: int) -> Check:
    c = Check(f"{layer}_row_count_equal")
    if airflow_count == dagster_count:
        return c.ok(f"{airflow_count:,} rows in both orchestrators")
    return c.fail(
        f"Airflow={airflow_count:,}  Dagster={dagster_count:,}  "
        f"delta={abs(airflow_count - dagster_count):,}"
    )


def check_gold_schema(airflow_schema: dict, dagster_schema: dict) -> Check:
    c = Check("gold_schema_compatible")
    airflow_fields = set(airflow_schema.keys())
    dagster_fields = set(dagster_schema.keys())
    missing = airflow_fields - dagster_fields
    extra = dagster_fields - airflow_fields
    issues = []
    if missing:
        issues.append(f"fields missing in Dagster: {sorted(missing)}")
    if extra:
        issues.append(f"extra fields in Dagster: {sorted(extra)}")
    # Check type compatibility for common fields
    type_mismatches = []
    for field in airflow_fields & dagster_fields:
        af_type = airflow_schema[field]
        dag_type = dagster_schema[field]
        # Normalise Decimal variants for comparison purposes
        def _base_type(t: str) -> str:
            t = t.lower()
            if t.startswith("decimaltype"):
                return "decimaltype"
            return t
        if _base_type(af_type) != _base_type(dag_type):
            type_mismatches.append(f"{field}: Airflow={af_type} Dagster={dag_type}")
    if type_mismatches:
        issues.append(f"type mismatches: {type_mismatches}")
    if issues:
        return c.fail("; ".join(issues))
    common_fields = sorted(airflow_fields & dagster_fields)
    return c.ok(f"Schema compatible – {len(common_fields)} shared fields: {common_fields}")


def check_gold_state_codes(airflow_rows: list, dagster_rows: list) -> Check:
    c = Check("gold_state_code_set_equal")
    af_codes = {r["State_Code"] for r in airflow_rows}
    dag_codes = {r["State_Code"] for r in dagster_rows}
    missing = af_codes - dag_codes
    extra = dag_codes - af_codes
    if not missing and not extra:
        return c.ok(f"{len(af_codes)} states present in both")
    issues = []
    if missing:
        issues.append(f"states in Airflow but not Dagster: {sorted(missing)}")
    if extra:
        issues.append(f"states in Dagster but not Airflow: {sorted(extra)}")
    return c.fail("; ".join(issues))


def check_gold_record_counts(airflow_rows: list, dagster_rows: list) -> Check:
    c = Check("gold_record_count_per_state_equal")
    af_map = {r["State_Code"]: r["record_count"] for r in airflow_rows}
    dag_map = {r["State_Code"]: r["record_count"] for r in dagster_rows}
    diffs = []
    for code in sorted(set(af_map) | set(dag_map)):
        af_v = af_map.get(code)
        dag_v = dag_map.get(code)
        if af_v != dag_v:
            diffs.append(f"State {code}: Airflow={af_v} Dagster={dag_v}")
    if not diffs:
        return c.ok(f"All {len(af_map)} state record_counts match exactly")
    return c.fail(f"{len(diffs)} mismatches: {diffs[:10]}")  # show first 10


def check_gold_bmi_mean(
    airflow_rows: list, dagster_rows: list, tolerance: float
) -> Check:
    c = Check("gold_bmi_mean_per_state_within_tolerance")
    af_map = {r["State_Code"]: r["bmi_mean"] for r in airflow_rows}
    dag_map = {r["State_Code"]: r["bmi_mean"] for r in dagster_rows}
    diffs = []
    for code in sorted(set(af_map) | set(dag_map)):
        af_v = af_map.get(code)
        dag_v = dag_map.get(code)
        if af_v is None and dag_v is None:
            continue
        if af_v is None or dag_v is None:
            diffs.append(f"State {code}: one value is None (Airflow={af_v} Dagster={dag_v})")
            continue
        delta = abs(af_v - dag_v)
        if delta > tolerance:
            diffs.append(
                f"State {code}: Airflow={af_v:.8f} Dagster={dag_v:.8f} delta={delta:.2e}"
            )
    if not diffs:
        return c.ok(
            f"All {len(af_map)} bmi_mean values within tolerance {tolerance}"
        )
    return c.fail(f"{len(diffs)} states exceed tolerance {tolerance}: {diffs[:10]}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_comparison(
    airflow_manifest_path: str | None,
    dagster_manifest_path: str | None,
    bmi_tolerance: float,
    report_path: str,
) -> int:
    """Execute all parity checks and write the report.  Returns 0=pass, 1=fail."""
    # ---- Resolve manifests -------------------------------------------------
    try:
        if airflow_manifest_path:
            af_manifest = _load_manifest_direct(airflow_manifest_path)
        else:
            af_manifest = _resolve_manifest(_DEFAULT_AIRFLOW_POINTER)
    except (FileNotFoundError, KeyError, ValueError) as exc:
        print(f"[ERROR] Could not load Airflow manifest: {exc}", file=sys.stderr)
        return 1

    try:
        if dagster_manifest_path:
            dag_manifest = _load_manifest_direct(dagster_manifest_path)
        else:
            dag_manifest = _resolve_manifest(_DEFAULT_DAGSTER_POINTER)
    except (FileNotFoundError, KeyError, ValueError) as exc:
        print(f"[ERROR] Could not load Dagster manifest: {exc}", file=sys.stderr)
        return 1

    af_paths = af_manifest["output_paths"]
    dag_paths = dag_manifest["output_paths"]

    print(f"Airflow run  : {af_manifest['run_id']}")
    print(f"Dagster run  : {dag_manifest['run_id']}")
    print(f"Input file   : {af_manifest.get('input_file', 'n/a')}")
    print()

    # ---- Start Spark --------------------------------------------------------
    # Add project root to path so spark.health_data_pipeline imports succeed
    if _PROJECT_ROOT not in sys.path:
        sys.path.insert(0, _PROJECT_ROOT)

    from spark.health_data_pipeline import get_spark_session  # noqa: PLC0415
    spark = get_spark_session("parity_checker")

    checks: list[Check] = []
    layer_counts: dict[str, dict[str, int]] = {}
    af_schema: dict[str, str] = {}
    dag_schema: dict[str, str] = {}
    af_gold: list[dict[str, Any]] = []
    dag_gold: list[dict[str, Any]] = []

    try:
        # ---- Row counts at each layer ----------------------------------------
        print("Checking row counts …")
        for layer in ("bronze", "silver", "gold"):
            af_count = _read_parquet_count(spark, af_paths[layer])
            dag_count = _read_parquet_count(spark, dag_paths[layer])
            layer_counts[layer] = {
                "airflow": af_count,
                "dagster": dag_count,
            }
            print(f"  {layer:6s}: Airflow={af_count:,}  Dagster={dag_count:,}")
            checks.append(check_row_count(layer, af_count, dag_count))

        # ---- Gold schema compatibility ----------------------------------------
        print("\nChecking gold schema …")
        af_schema = _read_parquet_schema(spark, af_paths["gold"])
        dag_schema = _read_parquet_schema(spark, dag_paths["gold"])
        print(f"  Airflow schema : {af_schema}")
        print(f"  Dagster schema : {dag_schema}")
        checks.append(check_gold_schema(af_schema, dag_schema))

        # ---- Gold state-level aggregates -------------------------------------
        print("\nLoading gold aggregates …")
        af_gold = _read_gold_rows(spark, af_paths["gold"])
        dag_gold = _read_gold_rows(spark, dag_paths["gold"])

        checks.append(check_gold_state_codes(af_gold, dag_gold))
        checks.append(check_gold_record_counts(af_gold, dag_gold))
        checks.append(check_gold_bmi_mean(af_gold, dag_gold, bmi_tolerance))

    finally:
        spark.stop()

    # ---- Summarise ----------------------------------------------------------
    passed = [c for c in checks if c.passed]
    failed = [c for c in checks if not c.passed]
    overall = "PASS" if not failed else "FAIL"

    print(f"\n{'='*60}")
    print(f"  PARITY RESULT: {overall}  ({len(passed)}/{len(checks)} checks passed)")
    print(f"{'='*60}")
    for c in checks:
        status = "PASS" if c.passed else "FAIL"
        print(f"  [{status}] {c.name}")
        if not c.passed:
            print(f"        {c.detail}")
    print()

    # ---- Write report -------------------------------------------------------
    af_gold_map = {r["State_Code"]: r for r in af_gold}
    dag_gold_map = {r["State_Code"]: r for r in dag_gold}
    all_codes = sorted(set(af_gold_map) | set(dag_gold_map))
    gold_state_comparison = []
    for code in all_codes:
        af_row = af_gold_map.get(code)
        dag_row = dag_gold_map.get(code)
        af_count = af_row["record_count"] if af_row else None
        dag_count = dag_row["record_count"] if dag_row else None
        af_bmi = af_row["bmi_mean"] if af_row else None
        dag_bmi = dag_row["bmi_mean"] if dag_row else None
        count_equal = af_count == dag_count
        if af_bmi is None and dag_bmi is None:
            bmi_delta = 0.0
            bmi_within_tolerance = True
        elif af_bmi is None or dag_bmi is None:
            bmi_delta = None
            bmi_within_tolerance = False
        else:
            bmi_delta = abs(af_bmi - dag_bmi)
            bmi_within_tolerance = bmi_delta <= bmi_tolerance
        gold_state_comparison.append(
            {
                "State_Code": code,
                "State_Name": (
                    af_row["State_Name"] if af_row and af_row["State_Name"]
                    else (dag_row["State_Name"] if dag_row else None)
                ),
                "airflow_record_count": af_count,
                "dagster_record_count": dag_count,
                "record_count_equal": count_equal,
                "airflow_bmi_mean": af_bmi,
                "dagster_bmi_mean": dag_bmi,
                "bmi_abs_delta": bmi_delta,
                "bmi_within_tolerance": bmi_within_tolerance,
            }
        )

    report = {
        "parity_result": overall,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "airflow_run_id": af_manifest["run_id"],
        "dagster_run_id": dag_manifest["run_id"],
        "input_file": af_manifest.get("input_file"),
        "bmi_tolerance": bmi_tolerance,
        "summary": {
            "total_checks": len(checks),
            "passed": len(passed),
            "failed": len(failed),
        },
        "counts": layer_counts,
        "gold_schema": {
            "airflow": af_schema,
            "dagster": dag_schema,
        },
        "gold_state_level_aggregates": gold_state_comparison,
        "checks": [c.to_dict() for c in checks],
    }
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w") as fh:
        json.dump(report, fh, indent=2)
    print(f"Parity report written to: {report_path}")

    return 0 if overall == "PASS" else 1


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--airflow-manifest",
        default=None,
        metavar="PATH",
        help="Direct path to an Airflow run_manifest.json (skips pointer lookup).",
    )
    p.add_argument(
        "--dagster-manifest",
        default=None,
        metavar="PATH",
        help="Direct path to a Dagster run_manifest.json (skips pointer lookup).",
    )
    p.add_argument(
        "--bmi-tolerance",
        type=float,
        default=1e-6,
        metavar="FLOAT",
        help="Max absolute difference allowed between bmi_mean values (default: 1e-6).",
    )
    p.add_argument(
        "--report-path",
        default=_DEFAULT_REPORT_PATH,
        metavar="PATH",
        help=f"Output path for the JSON parity report (default: {_DEFAULT_REPORT_PATH}).",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    sys.exit(
        run_comparison(
            airflow_manifest_path=args.airflow_manifest,
            dagster_manifest_path=args.dagster_manifest,
            bmi_tolerance=args.bmi_tolerance,
            report_path=args.report_path,
        )
    )
