"""Scheduled sandbox snapshot refresh; deploy with package.sh and deploy.py."""
import datetime as dt
import json
import os
from pathlib import Path
import subprocess
import tempfile

from refresh import build_database


def handler(event, context):
    import boto3

    # The packaged exporter reads live Sheets with Momento bypassed.
    with tempfile.TemporaryDirectory(prefix="f3-refresh-") as temporary:
        work = Path(temporary)
        export = work / "southfork.json"
        subprocess.run(
            ["/var/task/export/Export", str(export)],
            cwd="/var/task/export", check=True, timeout=240,
        )
        snapshot = work / "southfork.duckdb"
        refreshed_at = dt.datetime.now(dt.timezone.utc).isoformat()
        counts = build_database(json.loads(export.read_text()), snapshot, refreshed_at)
        if snapshot.stat().st_size > 64 * 1024 * 1024:
            raise ValueError("Snapshot exceeds the hosted reader's 64 MiB limit")
        latest = subprocess.check_output([
            "duckdb", "-readonly", "-csv", "-noheader", str(snapshot),
            "SELECT max(date) FROM posts",
        ], text=True, timeout=15).strip()
        # Single PUT publishes only the fully validated snapshot. Failed exports
        # and imports leave the previously published object untouched.
        with snapshot.open("rb") as body:
            boto3.client("s3").put_object(
                Bucket=os.environ["SNAPSHOT_BUCKET"], Key=os.environ["SNAPSHOT_KEY"],
                Body=body, ContentType="application/octet-stream",
                ServerSideEncryption="AES256",
                Metadata={"refreshed-at": refreshed_at, "latest-attendance": latest},
            )
        result = {"refreshedAt": refreshed_at, "latestAttendance": latest, "counts": counts}
        print(json.dumps(result))
        return result
