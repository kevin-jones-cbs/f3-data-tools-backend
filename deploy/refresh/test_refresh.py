import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools/southfork-duckdb"))
import lambda_function
import refresh


class RefreshTests(unittest.TestCase):
    def setUp(self):
        self.s3 = Mock()
        self.boto = patch.dict(sys.modules, {"boto3": types.SimpleNamespace(client=lambda _: self.s3)})
        self.boto.start()
        self.addCleanup(self.boto.stop)
        self.environment = patch.dict(lambda_function.os.environ, {
            "SNAPSHOT_BUCKET": "sandbox", "SNAPSHOT_PREFIX": "analytics/sandbox"})
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def export(self, command, **kwargs):
        Path(command[2]).write_text(json.dumps({"posts": [{"pax": command[1]}]}))

    def build(self, payload, path, refreshed_at, region):
        self.assertEqual(region, payload["posts"][0]["pax"])
        path.write_bytes(region.encode())
        return {"posts": 1}

    @patch.object(lambda_function.subprocess, "run")
    def test_failed_export_does_not_publish(self, run):
        run.side_effect = subprocess.TimeoutExpired("export", 240)
        with self.assertRaises(subprocess.TimeoutExpired):
            lambda_function.handler({}, None)
        self.s3.put_object.assert_not_called()

    @patch.object(lambda_function.subprocess, "check_output", return_value="2026-09-30\n")
    @patch.object(lambda_function, "build_database")
    @patch.object(lambda_function.subprocess, "run")
    def test_second_region_validation_failure_preserves_both_snapshots(self, run, build, query):
        run.side_effect = self.export
        def fail_goldrush(payload, path, refreshed_at, region):
            if region == "goldrush":
                raise ValueError("invalid data")
            return self.build(payload, path, refreshed_at, region)
        build.side_effect = fail_goldrush
        with self.assertRaises(ValueError):
            lambda_function.handler({}, None)
        self.assertEqual(2, build.call_count)
        self.s3.put_object.assert_not_called()

    @patch.object(lambda_function.subprocess, "check_output", return_value="2026-09-30\n")
    @patch.object(lambda_function, "build_database")
    @patch.object(lambda_function.subprocess, "run")
    def test_publishes_all_validated_files_with_correct_region_and_metadata(self, run, build, query):
        run.side_effect = self.export
        build.side_effect = self.build
        uploaded = []
        def put(**kwargs):
            self.assertEqual(len(refresh.SUPPORTED_REGIONS), build.call_count)
            uploaded.append({**kwargs, "bytes": kwargs["Body"].read()})
        self.s3.put_object.side_effect = put
        result = lambda_function.handler({}, None)
        self.assertEqual(list(refresh.SUPPORTED_REGIONS), list(result["regions"]))
        self.assertEqual(len(refresh.SUPPORTED_REGIONS), len(uploaded))
        for region, item in zip(result["regions"], uploaded):
            self.assertEqual(region.encode(), item["bytes"])
            self.assertEqual(f"analytics/sandbox/{region}.duckdb", item["Key"])
            self.assertEqual("AES256", item["ServerSideEncryption"])
            self.assertEqual("2026-09-30", item["Metadata"]["latest-attendance"])
            self.assertEqual({"posts": 1}, result["regions"][region]["counts"])

    @patch.object(lambda_function.subprocess, "check_output", return_value="2026-09-30\n")
    @patch.object(lambda_function, "build_database")
    @patch.object(lambda_function.subprocess, "run")
    def test_upload_failure_propagates_for_scheduler_retry(self, run, build, query):
        run.side_effect = self.export
        build.side_effect = self.build
        self.s3.put_object.side_effect = [None, OSError("upload failed")]
        with self.assertRaises(OSError):
            lambda_function.handler({}, None)


@unittest.skipUnless(shutil.which("duckdb"), "DuckDB CLI required for importer integration tests")
class ImportTests(unittest.TestCase):
    def test_isolated_region_snapshots_preserve_qsource_and_historical_data(self):
        with tempfile.TemporaryDirectory() as directory:
            for region in refresh.SUPPORTED_REGIONS:
                with self.subTest(region=region):
                    post = {"date": "2026-09-30T00:00:00", "site": "Test AO", "pax": "Test Pax",
                            "isQ": True, "isFNG": False, "extraActivity": False}
                    payload = {"posts": [post], "qSourcePosts": [post],
                               "pax": [{"name": "Test Pax", "dateJoined": "09/01/2026"}],
                               "aos": [{"name": "Test AO", "dayOfWeek": 3}],
                               "historicalData": [{"paxName": "Test Pax", "postCount": 10,
                                                   "qCount": 2, "firstPost": "2020-01-01T00:00:00"}]}
                    path = Path(directory) / f"{region}.duckdb"
                    refresh.build_database(payload, path, "2026-10-04T00:00:00+00:00", region)
                    tables = ["posts", "qsource_posts", "pax", "aos", "historical_totals", "import_metadata"]
                    sql = "SELECT DISTINCT region FROM (" + " UNION ALL ".join(
                        f"SELECT region FROM {table}" for table in tables) + ")"
                    result = json.loads(subprocess.check_output(["duckdb", "-readonly", "-json", str(path), sql]))
                    self.assertEqual([{"region": region}], result)
                    result = json.loads(subprocess.check_output(["duckdb", "-readonly", "-json", str(path),
                        "SELECT (SELECT count(*) FROM qsource_posts) AS qs, (SELECT post_count FROM historical_totals) AS historical"]))
                    self.assertEqual([{"qs": 1, "historical": 10}], result)
                    previous = path.read_bytes()
                    with self.assertRaises(ValueError):
                        refresh.build_database({"posts": []}, path, "2026-10-04T00:00:00+00:00", region)
                    self.assertEqual(previous, path.read_bytes())


if __name__ == "__main__":
    unittest.main()
