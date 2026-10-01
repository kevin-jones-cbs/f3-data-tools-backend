import json
from pathlib import Path
import subprocess
import sys
import types
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools/southfork-duckdb"))
import lambda_function


class RefreshTests(unittest.TestCase):
    def setUp(self):
        self.s3 = Mock()
        self.boto = patch.dict(sys.modules, {"boto3": types.SimpleNamespace(client=lambda _: self.s3)})
        self.boto.start()
        self.addCleanup(self.boto.stop)

    def export(self, command, **kwargs):
        Path(command[1]).write_text(json.dumps({"posts": [{"pax": "test"}]}))

    @patch.object(lambda_function.subprocess, "run")
    def test_failed_export_does_not_publish(self, run):
        run.side_effect = subprocess.TimeoutExpired("export", 240)
        with self.assertRaises(subprocess.TimeoutExpired):
            lambda_function.handler({}, None)
        self.s3.put_object.assert_not_called()

    @patch.object(lambda_function, "build_database", side_effect=ValueError("invalid data"))
    @patch.object(lambda_function.subprocess, "run")
    def test_failed_validation_keeps_previous_snapshot(self, run, build):
        run.side_effect = self.export
        with self.assertRaises(ValueError):
            lambda_function.handler({}, None)
        self.s3.put_object.assert_not_called()

    @patch.dict(lambda_function.os.environ, {"SNAPSHOT_BUCKET": "sandbox", "SNAPSHOT_KEY": "snapshot.duckdb"})
    @patch.object(lambda_function.subprocess, "check_output", return_value="2026-09-30\n")
    @patch.object(lambda_function, "build_database")
    @patch.object(lambda_function.subprocess, "run")
    def test_publishes_validated_file_with_freshness_metadata(self, run, build, query):
        run.side_effect = self.export
        def create(payload, path, refreshed_at):
            path.write_bytes(b"validated snapshot")
            return {"posts": 1}
        build.side_effect = create
        uploaded = {}
        def put(**kwargs):
            uploaded.update(kwargs)
            uploaded["bytes"] = kwargs["Body"].read()
        self.s3.put_object.side_effect = put
        result = lambda_function.handler({}, None)
        self.assertEqual(b"validated snapshot", uploaded["bytes"])
        self.assertEqual("snapshot.duckdb", uploaded["Key"])
        self.assertEqual("AES256", uploaded["ServerSideEncryption"])
        self.assertEqual("2026-09-30", uploaded["Metadata"]["latest-attendance"])
        self.assertEqual({"posts": 1}, result["counts"])


if __name__ == "__main__":
    unittest.main()
