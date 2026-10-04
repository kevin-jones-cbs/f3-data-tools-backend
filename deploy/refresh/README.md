# Daily sandbox attendance refresh: South Fork and Gold Rush

AWS EventBridge Scheduler runs `F3AnalyticsRefresh-sandbox-daily` at **05:00
America/Los_Angeles** every day (daylight-saving aware), in account `311293999880`,
region `us-west-1`. It invokes the private `F3AnalyticsRefresh-sandbox` Lambda;
no laptop or public HTTP endpoint is involved.

The Lambda runs the existing Sheets exporter with Momento bypassed, builds and
validates a separate DuckDB in `/tmp` for every region in
`tools/southfork-duckdb/supported_regions.json`, then uploads both snapshots:

- `s3://f3-data-tools-config-311293999880/analytics/sandbox/southfork.duckdb`
- `s3://f3-data-tools-config-311293999880/analytics/sandbox/goldrush.duckdb`

An export or validation failure preserves both previous objects. S3 uploads are
atomic per file; an upload failure can leave different refresh times until retry. Sandbox chat picks
up a changed object within five minutes of subsequent requests. Chat logs are
separate and unaffected.

The worker has a dedicated role permitting only writes to those snapshots and its
CloudWatch log streams. The scheduler role can invoke only this worker. The
worker copies the existing sandbox Sheets credential during deployment; redeploy
after rotating that credential or changing packaged region mappings. It has
1024 MiB memory, a ten-minute timeout, and concurrency limited to one. Scheduler
delivery retries are bounded to two within one hour; Lambda's normal asynchronous
execution retries also apply. CloudWatch logs are retained for 30 days. No email
alerts are configured.

The self-contained .NET exporter uses invariant globalization because the Python
Lambda runtime does not include ICU. Its Sheets dates are parsed with the same
month/day conventions expected by the existing snapshot importer.

## Deploy/update

From the backend directory, with .NET 10, Python 3, AWS CLI, curl, and zip/unzip:

```sh
bash deploy/refresh/package.sh /tmp/f3-sandbox-refresh.zip
python3 deploy/refresh/deploy.py --package /tmp/f3-sandbox-refresh.zip --profile kevin-personal
```

This idempotent script updates the worker, IAM policies, and schedule. It also
configures the shared snapshot prefix and sandbox-only S3 read permissions on the existing
chat Lambda, preserving its other environment settings. It checks
the AWS account before writes and uses permission-restricted temporary request
files that are deleted after each call; secrets never appear in command arguments
or committed files. The worker is deployed separately from
the normal chat Lambda pipeline. Deploy the current chat API before running this
script; it must support `F3_ANALYTICS_S3_PREFIX` and include `analytics-regions.json`.

## Run immediately / inspect

```sh
aws lambda invoke --function-name F3AnalyticsRefresh-sandbox \
  --payload '{}' --cli-binary-format raw-in-base64-out \
  --profile kevin-personal --region us-west-1 /tmp/f3-refresh-result.json
cat /tmp/f3-refresh-result.json

aws scheduler get-schedule --name F3AnalyticsRefresh-sandbox-daily \
  --profile kevin-personal --region us-west-1

aws logs tail /aws/lambda/F3AnalyticsRefresh-sandbox --since 1d \
  --profile kevin-personal --region us-west-1
```

Successful results include `regions`, with `refreshedAt`, `latestAttendance`, and
per-table counts for each region.
Inspect the invocation's `FunctionError` and result before treating HTTP 200 as
success. The chat `/chat/status?region=southfork` and `/chat/status?region=goldrush` endpoints show snapshot freshness
without calling the AI model. The existing local `refresh.py` command remains
available for manual refreshes.

Tests: `python3 -B -m unittest discover -s deploy/refresh -p 'test_*.py'`.

Snapshot freshness uses the latest attendance date on or before today, excluding
legacy future-date markers such as 2099. The exported source rows remain intact.
