#!/usr/bin/env python3
"""Refresh a local South Fork DuckDB using the backend's read-only Sheets action."""
import datetime as dt
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

HERE = Path(__file__).resolve().parent
BACKEND = HERE.parent.parent


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def build_database(payload, destination, refreshed_at):
    """Build and validate a new snapshot before replacing the previous database."""
    if not isinstance(payload.get("posts"), list) or not payload["posts"]:
        raise ValueError("Refusing to replace database with empty/missing posts")
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    specs = {
        "posts": ("posts", {
            "date": "TIMESTAMP", "site": "VARCHAR", "pax": "VARCHAR",
            "isQ": "BOOLEAN", "isFNG": "BOOLEAN", "extraActivity": "BOOLEAN",
        }, "CAST(date AS DATE) AS date, site AS ao, pax, isQ AS is_q, isFNG AS is_fng, extraActivity AS extra_activity"),
        "qsource_posts": ("qSourcePosts", {
            "date": "TIMESTAMP", "site": "VARCHAR", "pax": "VARCHAR",
            "isQ": "BOOLEAN", "isFNG": "BOOLEAN", "extraActivity": "BOOLEAN",
        }, "CAST(date AS DATE) AS date, site AS ao, pax, isQ AS is_q, isFNG AS is_fng, extraActivity AS extra_activity"),
        "pax": ("pax", {
            "name": "VARCHAR", "dateJoined": "VARCHAR", "namingRegion": "VARCHAR", "ehdByPaxName": "VARCHAR",
        }, "name, CAST(strptime(nullif(dateJoined, ''), '%m/%d/%Y') AS DATE) AS date_joined, namingRegion AS naming_region, ehdByPaxName AS ehd_by"),
        "aos": ("aos", {
            "name": "VARCHAR", "city": "VARCHAR", "dayOfWeek": "INTEGER",
            "hasQSource": "BOOLEAN", "isQSourceOnly": "BOOLEAN",
        }, "name, city, dayOfWeek AS day_of_week, hasQSource AS has_qsource, isQSourceOnly AS is_qsource_only"),
        "historical_totals": ("historicalData", {
            "paxName": "VARCHAR", "postCount": "INTEGER", "qCount": "INTEGER", "firstPost": "TIMESTAMP",
        }, "paxName AS pax, postCount AS post_count, qCount AS q_count, CAST(firstPost AS DATE) AS first_post"),
    }
    with tempfile.TemporaryDirectory(dir=destination.parent) as temporary:
        work = Path(temporary)
        database = work / "snapshot.duckdb"
        statements = ["BEGIN TRANSACTION;"]
        for table, (key, columns, selection) in specs.items():
            rows = payload.get(key) or []
            source = work / f"{table}.json"
            source.write_text(json.dumps(rows), encoding="utf-8")
            types = ", ".join(f"{literal(k)}: {literal(v)}" for k, v in columns.items())
            statements.append(
                f"CREATE TABLE {table} AS SELECT 'southfork' AS region, {selection} "
                f"FROM read_json({literal(source)}, format='array', columns={{{types}}});"
            )
        statements.append(f"""
            CREATE TABLE import_metadata AS SELECT
                'southfork' AS region,
                {literal(refreshed_at)}::TIMESTAMPTZ AS refreshed_at,
                'Google Sheets via local F3Lambda GetAllPosts; cache bypassed' AS source,
                1 AS schema_version;
            CREATE VIEW monthly_attendance AS
                SELECT region, date_trunc('month', date)::DATE AS month, ao,
                    count(*) AS posts, count(DISTINCT pax) AS unique_pax,
                    count(*) FILTER (WHERE is_q) AS qs
                FROM posts GROUP BY region, month, ao;
            CREATE VIEW pax_summary AS
                SELECT region, pax, count(*) AS posts,
                    count(*) FILTER (WHERE is_q) AS qs,
                    count(DISTINCT ao) AS aos_visited,
                    min(date) AS first_post, max(date) AS last_post
                FROM posts GROUP BY region, pax;
            COMMIT;
        """)
        subprocess.run(["duckdb", "-bail", str(database)], input="\n".join(statements), text=True, check=True)
        checks = " UNION ALL ".join(
            f"SELECT '{table}' AS name, count(*) AS rows FROM {table}" for table in specs
        )
        actual = json.loads(subprocess.check_output(
            ["duckdb", "-readonly", "-json", str(database), checks], text=True))
        expected = {table: len(payload.get(spec[0]) or []) for table, spec in specs.items()}
        if {row["name"]: row["rows"] for row in actual} != expected:
            raise ValueError("Imported counts do not match the backend export")
        invalid = subprocess.check_output([
            "duckdb", "-readonly", "-csv", "-noheader", str(database),
            "SELECT count(*) FROM posts WHERE date IS NULL OR nullif(trim(pax), '') IS NULL OR nullif(trim(ao), '') IS NULL;",
        ], text=True).strip()
        if invalid != "0":
            raise ValueError(f"Found {invalid} attendance rows missing date, PAX or AO")
        os.replace(database, destination)
        return expected


def main():
    for executable in ("duckdb", "dotnet"):
        if not shutil.which(executable):
            raise SystemExit(f"Required executable not found: {executable}")
    data = HERE / "data"
    data.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(dir=data) as temporary:
        export = Path(temporary) / "southfork.json"
        subprocess.run([
            "dotnet", "run", "--project", str(HERE / "Export" / "Export.csproj"),
            "--", str(export),
        ], cwd=BACKEND / "F3Lambda", check=True)
        counts = build_database(json.loads(export.read_text()), data / "southfork.duckdb",
                                dt.datetime.now(dt.timezone.utc).isoformat())
    print(f"Database: {data / 'southfork.duckdb'}")
    for table, count in counts.items():
        print(f"  {table}: {count:,} rows")


if __name__ == "__main__":
    main()
