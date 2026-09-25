# f3-data-tools-backend
AWS Lambda function that powers F3 Data Tools.

## Action Routing

Lambda action names are defined in `F3Core.LambdaActions`, not as ad hoc string literals. The Lambda handler dispatches from that shared registry, and frontend callers should set `FunctionInput.Action` with `LambdaActions.*` constants from `F3Core.dll`.

When adding a backend action:

1. Add the action constant and metadata in `f3-data-tools-core/LambdaActions.cs`.
2. Build core and copy the updated `F3Core.dll` into this repo's `F3Lambda/Packages/`.
3. Add the action handler in `F3Lambda/Function.cs`.
4. Mark `includeInSmokeTests: true` only for read-only actions that are safe to run across all regions.

## Smoke Tests

`F3Lambda.SmokeTests` is a console smoke runner for exercising read-only Lambda actions across regions. It can call the Lambda locally in-process or call the deployed dev Lambda URL.

```bash
# Local Lambda invocation. Momento is skipped by default.
dotnet run --project F3Lambda.SmokeTests/F3Lambda.SmokeTests.csproj -- --target local --timeout 180

# Deployed dev Lambda URL.
dotnet run --project F3Lambda.SmokeTests/F3Lambda.SmokeTests.csproj -- --target dev --timeout 120 --concurrency 3

# Local and dev for selected regions.
dotnet run --project F3Lambda.SmokeTests/F3Lambda.SmokeTests.csproj -- --target local,dev --regions southfork,rubicon
```

Useful options:

- `--regions southfork,rubicon` limits region-scoped checks.
- `--actions GetPax,GetLocations` limits action coverage.
- `--url https://...` overrides the dev Lambda URL. `F3_SMOKE_DEV_URL` works too.
- `--use-cache` keeps Momento enabled for local checks.
- `--include-expensive` adds uncached local checks for `GetInitialView`, `GetRegionSummary`, and sector aggregate data.

Local smoke tests require `GOOGLE_SVC_ACT_JSON` or `F3Lambda/Secrets/SvcAct.json`. By default, local mode sets `F3_SKIP_MOMENTO=true` so smoke checks prove the Google Sheets path works instead of passing from cached data.

## Cache Bypass

Set `F3_SKIP_MOMENTO=true` (also accepts `1` or `yes`) to skip Momento reads, writes, and clears. This is intended for local smoke testing and other validation where cached data would hide a broken live Google Sheets call.

## Region Configuration Editor

The editor uses the Google Sheet's own editor permission as its authorization boundary. Production requires:

- `REGION_CONFIG_BUCKET` and optionally `REGION_CONFIG_KEY` (defaults to `regions.json`).
- `REGION_CONFIG_SESSION_SECRET`, a random value of at least 32 characters.
- `GOOGLE_REGION_EDITOR_CLIENT_ID`, a Google Identity Services web client configured for the production and localhost frontend origins.
- `GOOGLE_PICKER_API_KEY`, restricted to the configured origins and Google Picker API.
- The Drive API and Picker API enabled in the same Google Cloud project.
- Lambda S3 permissions for `GetObject` and conditional `PutObject`; bucket versioning remains the rollback history.

The browser requests `openid email profile` and `drive.file`. It keeps the access token in page memory and sends it only to permission-gated editor actions. The Lambda must never log request bodies because they can contain this credential.

Local mode is read/validate-only by default. Point `REGION_CONFIG_FILE` at an explicit development catalog and set `REGION_CONFIG_ALLOW_LOCAL_WRITES=true` only when local saves are intentional. Local writes are atomic and use a content hash for optimistic concurrency. Never configure a local frontend to use production S3 credentials.

### Direct-S3 recovery

When the configured authorization spreadsheet is deleted or inaccessible, use the operator-only break-glass path. Preserve the downloaded file, validate it, and review the diff before uploading:

```sh
aws s3api get-object --bucket f3-data-tools-config-311293999880 --key regions.json --profile kevin-personal ./regions.recovery.json
jq empty ./regions.recovery.json
aws s3api put-object --bucket f3-data-tools-config-311293999880 --key regions.json --body ./regions.recovery.json --content-type application/json --profile kevin-personal
```

S3 versioning allows a prior `--version-id` to be downloaded and inspected, then uploaded as a new current version. Direct uploads bypass application validation and audit events, so retain an operator note and the relevant CloudTrail/S3 version metadata.

## Sandbox analytics chat deployment

See [Lambda chat deployment](deploy/chat/README.md) for the existing sandbox
Lambda streaming host, S3 snapshot caching, configuration, and manual snapshot refresh.
Local DuckDB development remains supported. Hosted saved-chat persistence is
not yet configured; the local telemetry database is unchanged.
