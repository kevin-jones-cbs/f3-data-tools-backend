# Local regional DuckDB snapshots

Run from this directory to refresh **South Fork, Gold Rush, SacTown, and Mother Lode**:

```sh
python3 refresh.py
duckdb -readonly data/southfork.duckdb
# Or query Gold Rush:
duckdb -readonly data/goldrush.duckdb
```

At the SQL prompt, try `SHOW TABLES;`, `DESCRIBE posts;`, or:

```sql
SELECT ao, count(*) AS posts
FROM posts
GROUP BY ao
ORDER BY posts DESC;
```

Run the example queries without opening an interactive prompt:

```sh
duckdb -readonly data/southfork.duckdb < queries.sql
```

Prerequisites: DuckDB CLI, Python 3 (standard library only), .NET 10 SDK,
and the backend's existing `GOOGLE_SVC_ACT_JSON` or `F3Lambda/Secrets/SvcAct.json`.
The exporter uses `F3Lambda/regions.json` for region configuration and invokes
only `GetAllPosts` locally, with Momento bypassed. It reads live Google Sheets;
it does not deploy or modify Sheets, cloud region configuration, or caches.

Local refresh is manual; the hosted sandbox refreshes daily at 05:00 Pacific via
[AWS Scheduler and a dedicated Lambda](../../deploy/refresh/README.md).
Close open DuckDB sessions before refreshing locally, then reopen to
see the new snapshot. The database is replaced only after a successful import
and validation. Data files and temporary exports are ignored by Git; credentials
are not copied into the database. No database server or AI service is required.

## Tables and views

| Name | Meaning |
| --- | --- |
| `posts` | One regular attendance record per PAX, date, and AO as returned by the app; includes `is_q`, `is_fng`, `extra_activity` |
| `qsource_posts` | QSource attendance, kept separate from regular posts |
| `pax` | Roster names, join dates, naming regions, and EH attribution |
| `aos` | Current AO schedule from the app; Sunday = 0 through Saturday = 6 |
| `historical_totals` | Aggregate historical counts when supplied; empty if the region has none |
| `import_metadata` | UTC refresh time, source, and schema version |
| `monthly_attendance` | Monthly attendance, unique PAX, and Q counts by AO |
| `pax_summary` | Regular attendance and Q totals, first/last dates, and AOs visited |

All tables include the snapshot's region ID (`southfork`, `goldrush`, `sactown`, or `motherlode`). Names and attendance multiplicity are
preserved from the backend; the import does not silently deduplicate or merge
people. Attendance names may include downrange PAX absent from the roster.
The AO schedule can have multiple rows per name and excludes retired AOs under
the backend's existing rules; avoid joining it directly to attendance by name
without accounting for that multiplicity. Views do not combine historical totals
or QSource attendance with regular posts. Import counts are checked against the
backend export, and missing attendance dates, names, or AOs fail the import.

## Supported regions

`supported_regions.json` is the shared list for local refresh, the scheduled worker,
and deployment permissions/settings. Each normal refresh builds every listed region
and validates all snapshots before replacing or uploading any. Files stay separate.
To add a region, add its existing application region ID to this list, redeploy the
refresh worker and chat API, and run the worker. The chat API packages this same
list for local defaults and hosted S3 paths.
S3 publication is atomic per file, not across files; an upload failure can leave
regions with different refresh times until the retry succeeds.
