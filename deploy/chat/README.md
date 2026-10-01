# Sandbox chat deployment

The existing `F3Pax-sandbox` Lambda serves both the legacy data API at POST `/`
and the new `/chat/status`, `/chat`, and `/chat/stream` routes. Its existing
Function URL is retained. Production `F3Pax` is unchanged.

## Automatic deployment

Push the backend `sandbox` branch. GitHub Actions tests the backend, runs
`deploy/chat/package.sh`, uploads the ZIP to the existing deployment bucket,
and updates `F3Pax-sandbox`. The package includes a self-contained .NET 10
ASP.NET executable and DuckDB 1.5.3 for Linux x86_64. Local secrets and databases
are excluded. The `main` branch retains its existing production deployment.

Push the frontend `sandbox` branch to trigger Amplify. `ChatApiUrl` and
`LambdaUrl` both point to the sandbox Lambda URL. Development settings continue
to use the local backend.

## Lambda configuration

One-time hosting settings on `F3Pax-sandbox`:

- Runtime: dotnet10, architecture x86_64
- Handler: `F3Lambda.LocalApi`
- Layer: `arn:aws:lambda:us-west-1:753240598075:layer:LambdaAdapterLayerX86:30`
- Memory: 1024 MB; timeout: 180 seconds
- Function URL invoke mode: RESPONSE_STREAM
- `AWS_LAMBDA_EXEC_WRAPPER=/opt/bootstrap`
- `ASPNETCORE_URLS=http://+:8080`
- `AWS_LWA_PORT=8080`
- `AWS_LWA_READINESS_CHECK_PATH=/health`
- `AWS_LWA_INVOKE_MODE=response_stream`
- `DUCKDB_EXECUTABLE=/var/task/duckdb`

Existing Lambda environment variables and Function URL CORS origins are
preserved. The Function URL owns hosted CORS; ASP.NET supplies local CORS only.
No Secrets Manager setup is required. Set `OPENROUTER_API_KEY` directly in Lambda
Configuration > Environment variables. `OPENROUTER_MODEL` selects the model;
`OPENROUTER_ALLOWED_MODELS` optionally allows additional model IDs.

Snapshot setting:

```
F3_ANALYTICS_REGIONS__southfork=s3://f3-data-tools-config-311293999880/analytics/sandbox/southfork.duckdb
```

The execution role must have `s3:GetObject` for that exact object. The sandbox
snapshot permission is restricted by `lambda:SourceFunctionArn` so the shared
role does not give the production function access to the sandbox snapshot.

## Cached snapshots

`F3_ANALYTICS_REGIONS` accepts local paths or `s3://bucket/key` per region.
Local development defaults to the existing attendance file. S3 uses the AWS
SDK credential chain, including the execution role on Lambda. DuckDB receives
no AWS credentials and runs read-only with external access disabled.

The first request downloads the snapshot. Warm instances check S3's ETag at
most once every five minutes during normal operation. If-Match prevents a
concurrent upload from mixing versions. The download length, required metadata,
nonempty attendance, and region are validated before publishing an immutable
cached file. Each answer pins one version for all its queries and hash.

Failed refreshes fail the request without publishing the bad file or deleting
the prior snapshot. Subsequent requests retry. Downloads time out after 30
seconds and are limited to 64 MiB. Old versions are retained for in-flight
readers; the cache is capped at 256 MiB per region. Recycle the environment if
it fills. Cache location defaults to `/tmp/f3-analytics` on Lambda and can be
overridden with `F3_ANALYTICS_CACHE_PATH`.

## Refresh

From the backend directory, refresh locally, verify it succeeds, then upload:

```sh
python3 tools/southfork-duckdb/refresh.py
aws s3 cp tools/southfork-duckdb/data/southfork.duckdb \
  s3://f3-data-tools-config-311293999880/analytics/sandbox/southfork.duckdb \
  --profile kevin-personal --region us-west-1
```

Later requests pick up the upload within five minutes. No deployment is needed.
Only the attendance file is uploaded by this refresh command; the frozen eval
fixture and local chat logs remain local. A separate
[scheduled refresh Lambda](../refresh/README.md) refreshes the sandbox snapshot
daily at 05:00 America/Los_Angeles through AWS EventBridge Scheduler.

## Hosted chat telemetry

Sandbox saves each completed, failed, or canceled chat turn as a separate JSON
object at `s3://f3-data-tools-config-311293999880/chat-logs/sandbox/yyyy/MM/dd/{id}.json`.
Dates use UTC. Objects contain the existing camel-case `ChatTrace` format: request
and anonymous IDs, response, model calls and usage, query attempts, timing, and
errors. Validation rejections and requests rejected as busy do not create traces.
The bucket blocks public access and writes explicitly use S3-managed encryption.
No expiry or automatic deletion is configured.

Lambda settings:

```
F3_CHAT_LOG_S3_URI=s3://f3-data-tools-config-311293999880/chat-logs/sandbox/
```

The execution role's `F3SandboxChatTelemetryWrite` inline policy is checked in as
`telemetry-policy.json`. It permits only `s3:PutObject` within this prefix, with
the same sandbox source-function restriction used by snapshot access. It grants
no log reading or deletion permissions. Local viewing uses the `kevin-personal`
AWS profile, with no AWS credentials sent to the browser.

Writes are awaited before request completion with a separate five-second timeout,
including when the client disconnects. Storage failures log a warning and do not
replace the chat result. This is best-effort telemetry: a hard Lambda termination
or failed S3 write can lose a turn. There is no shared writable DuckDB in Lambda.

## Verification and current limits

Check `/health`, then `/chat/status?region=southfork`, then the sandbox frontend
at `https://sandbox.d82d0zhpulmga.amplifyapp.com/chat/southfork`.
Status returns `configured: false` until the OpenRouter key and model are set.
A status request validates the snapshot without invoking a paid AI model.

Local development still saves the separate telemetry DuckDB. Hosted chat logs
are stored privately in S3; `/admin/chats` returns 503 on Lambda.
The member-facing Function URL remains unauthenticated, as before.
