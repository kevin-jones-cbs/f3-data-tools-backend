"""Refresh every supported sandbox region; deploy with package.sh and deploy.py."""
import datetime as dt
import json
import os
from pathlib import Path
import subprocess
import tempfile

from refresh import SUPPORTED_REGIONS, build_database


def handler(event, context):
    import boto3

    # Build every region before publishing any snapshot. A Sheets/validation
    # failure preserves the complete previous set. S3 uploads are per object;
    # an upload failure can publish a partial set and is retried by the scheduler.
    with tempfile.TemporaryDirectory(prefix="f3-refresh-") as temporary:
        work = Path(temporary)
        results = {}
        for region in SUPPORTED_REGIONS:
            export = work / f"{region}.json"
            subprocess.run(
                ["/var/task/export/Export", region, str(export)],
                cwd="/var/task/export", check=True, timeout=240,
            )
            snapshot = work / f"{region}.duckdb"
            refreshed_at = dt.datetime.now(dt.timezone.utc).isoformat()
            counts = build_database(json.loads(export.read_text()), snapshot, refreshed_at, region)
            if snapshot.stat().st_size > 64 * 1024 * 1024:
                raise ValueError(f"{region} snapshot exceeds the hosted reader's 64 MiB limit")
            latest = subprocess.check_output([
                "duckdb", "-readonly", "-csv", "-noheader", str(snapshot),
                "SELECT max(date) FROM posts",
            ], text=True, timeout=15).strip()
            results[region] = {"refreshedAt": refreshed_at, "latestAttendance": latest, "counts": counts}

        s3 = boto3.client("s3")
        prefix = os.environ["SNAPSHOT_PREFIX"].strip("/")
        for region, result in results.items():
            with (work / f"{region}.duckdb").open("rb") as body:
                s3.put_object(
                    Bucket=os.environ["SNAPSHOT_BUCKET"], Key=f"{prefix}/{region}.duckdb",
                    Body=body, ContentType="application/octet-stream",
                    ServerSideEncryption="AES256",
                    Metadata={"refreshed-at": result["refreshedAt"],
                              "latest-attendance": result["latestAttendance"]},
                )
        result = {"regions": results}
        print(json.dumps(result))
        return result
