import json
import os
import tempfile
import unittest
from unittest.mock import patch

from dagster_pipeline.definitions import defs


class _FakeSparkSession:
    pass


class DagsterDirectJobSmokeTests(unittest.TestCase):
    def test_job_is_registered(self) -> None:
        """Smoke check: the direct Dagster parity job is resolvable from Definitions."""
        job = defs.get_job_def("health_data_pipeline_job")
        self.assertEqual(job.name, "health_data_pipeline_job")

    def test_direct_job_executes_with_mocked_spark_wrappers(self) -> None:
        """Smoke check: full asset chain materializes and writes run pointers/manifest."""
        with tempfile.TemporaryDirectory() as tmpdir:
            run_calls = {
                "run_bronze": [],
                "run_silver": [],
                "run_gold": [],
            }

            def _bronze_side_effect(_spark, _input_csv, run_output_dir):
                run_calls["run_bronze"].append((run_output_dir,))
                bronze_path = os.path.join(run_output_dir, "bronze", "raw_data.parquet")
                os.makedirs(bronze_path, exist_ok=True)
                return bronze_path

            def _silver_side_effect(_spark, bronze_path, run_output_dir):
                run_calls["run_silver"].append((bronze_path, run_output_dir))
                silver_path = os.path.join(run_output_dir, "silver", "filtered_data.parquet")
                os.makedirs(silver_path, exist_ok=True)
                return silver_path

            def _gold_side_effect(_spark, silver_path, run_output_dir, _ref_csv):
                run_calls["run_gold"].append((silver_path, run_output_dir))
                gold_path = os.path.join(run_output_dir, "gold", "aggregated_by_state.parquet")
                os.makedirs(gold_path, exist_ok=True)
                return gold_path

            with patch("dagster_pipeline.assets.DAGSTER_BASE_OUTPUT_DIR", tmpdir), patch(
                "dagster_pipeline.assets.DEFAULT_INPUT_CSV", "data/2013.csv"
            ), patch(
                "dagster_pipeline.assets.DEFAULT_REF_STATE_CODES_CSV", "data/ref_state_codes.csv"
            ), patch(
                "dagster_pipeline.assets.get_spark_session", return_value=_FakeSparkSession()
            ), patch(
                "dagster_pipeline.assets.run_bronze", side_effect=_bronze_side_effect
            ), patch(
                "dagster_pipeline.assets.run_silver", side_effect=_silver_side_effect
            ), patch(
                "dagster_pipeline.assets.run_gold", side_effect=_gold_side_effect
            ):
                result = defs.get_job_def("health_data_pipeline_job").execute_in_process()

            self.assertTrue(result.success)

            # Smoke: wrappers were invoked exactly once in the direct asset chain.
            self.assertEqual(len(run_calls["run_bronze"]), 1)
            self.assertEqual(len(run_calls["run_silver"]), 1)
            self.assertEqual(len(run_calls["run_gold"]), 1)

            latest_pointer_path = os.path.join(tmpdir, "latest_run.json")
            self.assertTrue(os.path.exists(latest_pointer_path))

            with open(latest_pointer_path) as fh:
                latest = json.load(fh)
            self.assertIn("run_id", latest)
            self.assertIn("manifest_path", latest)

            self.assertTrue(os.path.exists(latest["manifest_path"]))
            with open(latest["manifest_path"]) as fh:
                manifest = json.load(fh)

            self.assertEqual(manifest["orchestrator"], "dagster")
            self.assertEqual(manifest["run_id"], latest["run_id"])

    def test_regression_shared_wrapper_path_contract(self) -> None:
        """Regression: ensure Silver/Gold wrappers keep using run-scoped output dirs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            captured = {}

            def _bronze_side_effect(_spark, _input_csv, run_output_dir):
                bronze_path = os.path.join(run_output_dir, "bronze", "raw_data.parquet")
                os.makedirs(bronze_path, exist_ok=True)
                return bronze_path

            def _silver_side_effect(_spark, bronze_path, run_output_dir):
                captured["silver_args"] = (bronze_path, run_output_dir)
                silver_path = os.path.join(run_output_dir, "silver", "filtered_data.parquet")
                os.makedirs(silver_path, exist_ok=True)
                return silver_path

            def _gold_side_effect(_spark, silver_path, run_output_dir, _ref_csv):
                captured["gold_args"] = (silver_path, run_output_dir)
                gold_path = os.path.join(run_output_dir, "gold", "aggregated_by_state.parquet")
                os.makedirs(gold_path, exist_ok=True)
                return gold_path

            with patch("dagster_pipeline.assets.DAGSTER_BASE_OUTPUT_DIR", tmpdir), patch(
                "dagster_pipeline.assets.DEFAULT_INPUT_CSV", "data/2013.csv"
            ), patch(
                "dagster_pipeline.assets.DEFAULT_REF_STATE_CODES_CSV", "data/ref_state_codes.csv"
            ), patch(
                "dagster_pipeline.assets.get_spark_session", return_value=_FakeSparkSession()
            ), patch(
                "dagster_pipeline.assets.run_bronze", side_effect=_bronze_side_effect
            ), patch(
                "dagster_pipeline.assets.run_silver", side_effect=_silver_side_effect
            ), patch(
                "dagster_pipeline.assets.run_gold", side_effect=_gold_side_effect
            ):
                result = defs.get_job_def("health_data_pipeline_job").execute_in_process()

            self.assertTrue(result.success)

            self.assertIn("silver_args", captured)
            self.assertIn("gold_args", captured)
            silver_bronze_path, silver_output_dir = captured["silver_args"]
            gold_silver_path, gold_output_dir = captured["gold_args"]

            # Shared-wrapper regression guard: keep passing run-scoped output dir.
            self.assertEqual(
                silver_output_dir,
                os.path.dirname(os.path.dirname(silver_bronze_path)),
            )
            self.assertEqual(
                gold_output_dir,
                os.path.dirname(os.path.dirname(gold_silver_path)),
            )


if __name__ == "__main__":
    unittest.main()
