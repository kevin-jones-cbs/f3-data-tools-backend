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
