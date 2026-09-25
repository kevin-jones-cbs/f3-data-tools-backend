# Local South Fork chat

The Blazor page `/chat/southfork` talks to `F3Lambda.LocalApi` at port 5055.
The API uses OpenRouter tool calling and the installed DuckDB CLI to query the
local attendance snapshot. The key stays on the backend. No Sheets reads happen
during chat, and no chat history is persisted.

## Run

Refresh the snapshot if needed with `python3 tools/southfork-duckdb/refresh.py`.
Put your key and chosen model in `F3Lambda/Secrets/chat.local.json`:

```json
{
  "OPENROUTER_API_KEY": "your-key",
  "OPENROUTER_MODEL": "provider/your-tool-capable-model"
}
```

This file is ignored by Git and read only by the local backend. Create it if
missing. Restart the backend after editing it. Environment variables override
file settings, so the eval harness can select a different snapshot or model.
Then, from the backend repository:

```sh
dotnet run --project F3Lambda.LocalApi --no-launch-profile --urls http://localhost:5055
```

In another terminal, from the frontend repository:

```sh
dotnet run --project F3Wasm --no-launch-profile --urls http://localhost:5090
```

Open **http://localhost:5090/chat/southfork**. An **Ask South Fork** link is also
available in South Fork's navigation. Until the model and key are configured,
the page shows an unavailable state rather than accepting questions that fail.
Models must support tools. Choose the model explicitly; none is silently chosen
or substituted. Prompts and query results are sent to the chosen OpenRouter
provider to generate answers. SQL is never shown in the member-facing UI.

## Configuration

All settings below can be JSON properties in that file or environment variables.

| Setting | Purpose |
| --- | --- |
| `OPENROUTER_API_KEY` | Required backend-only credential |
| `OPENROUTER_MODEL` | Required default OpenRouter model ID |
| `OPENROUTER_ALLOWED_MODELS` | Additional comma-separated IDs permitted for eval overrides |
| `F3_ANALYTICS_DB_PATH` | Optional absolute snapshot path; defaults to the South Fork local database |
| `DUCKDB_EXECUTABLE` | Optional DuckDB CLI path; defaults to `duckdb` on PATH |

Frontend `ChatApiUrl` in `wwwroot/appsettings.Development.json` can override
`http://localhost:5055/`. This URL is public configuration, never a place for keys.
Existing application data calls keep their separate `LambdaUrl` configuration.

## Contracts and limits

- `GET /chat/status`: configuration readiness and snapshot dates/fingerprint.
- `POST /chat`: `{ "messages": [{ "role": "user", "content": "..." }], "model": "optional/allowed-id" }`.
- Response: `answer`, `model`, `snapshot`, `visualizations`, and diagnostic `queries`.
  The UI consumes only the answer, snapshot, and structured visualizations.
  Diagnostic query traces are for evals/developers, not displayed in chat.
- A visualization contains `kind`, `title`, `columns` (`key`, `label`), `rows`, and
  `truncated`. Today `kind=table`; future chart renderers can add category/series
  mappings and use the existing Blazorise.Charts dependency. No graph rendering
  or AI-generated HTML is included. The final successful query supplies the table;
  if the last query attempt fails, no stale earlier table is shown.
- Read-only database process, external access and extension loading disabled,
  locked configuration, stripped subprocess credentials, 256 MB query memory,
  10-second query timeout, 100 returned rows, bounded output, five tool attempts,
  and 120-second overall request deadline. Two chats run concurrently.
- This is a **local prototype**, not a deployed public endpoint. Before exposing
  paid chat publicly, integrate authentication, per-user quotas, and hosting
  controls. The LocalApi host's existing Sheets routes are not a public gateway.

## Extending to S3 / Lambda

`IAnalyticsSnapshotProvider` is the storage seam. An S3 provider can resolve a
versioned object, download it atomically into Lambda `/tmp`, and return the local
file path. DuckDB and chat logic stay the same. Pin one object version for a chat
request and reuse downloaded versions across warm invocations. Package the Linux
DuckDB binary for Lambda and configure its memory/timeout before deployment.
The implementation currently reads local files only; no bucket or cloud resource
is provisioned. Close query sessions before manually refreshing local snapshots.

## Verification and model comparisons

Run backend tests with `dotnet test F3Lambda.Tests/F3Lambda.Tests.csproj`.
See [the eval harness](../../tools/chat-evals/README.md) for frozen 2023–2025 cases,
baseline verification, and comparing models. Use a separate backend process with
`F3_ANALYTICS_DB_PATH` pointing to its frozen fixture. Fixed years alone don't
protect against edits to historical Sheets rows; the fixture fingerprint does.

Live answers need model-quality evaluation: structured query results are grounded
in DuckDB, while explanatory prose remains model-generated. The test harness
scores result correctness and records prose for human review.


### Streaming chat

The frontend uses `POST /chat/stream` with the same request body as `/chat`.
It returns newline-delimited JSON events: `status` (progress/reset provisional text),
`answer` (cumulative public answer text), `complete` (the final reply/table), or
`error` (member-safe message). Response bodies are flushed after every event.
OpenRouter SSE deltas are assembled for tool execution and telemetry; reasoning
and tool arguments are not sent as answer events. Usage from the final accounting
frame is retained. Disconnects and incomplete provider streams are errors, not
successful answers. Cancellation propagates to upstream reads.

The original JSON `/chat` endpoint remains available for the eval harness. Streaming
has deterministic parser/orchestration tests and Chrome tests with a delayed local
response server. Deployment through Lambda/proxies still needs streaming-capable
hosting; these changes enable the existing local ASP.NET backend.

### Regions

The frontend route `/chat/{region}` supplies `region` in `/chat` and `/chat/stream`
requests and in `/chat/status?region=...`. Labels and prompts use that region;
changing routes clears prior conversation context. Saved events include a server-
validated region, and the admin list/details display it. Legacy South Fork logs
are identified from their stored system prompt; unidentifiable logs show Unknown.

Configure one isolated snapshot per region in `Secrets/chat.local.json`:

```json
{
  "F3_ANALYTICS_REGIONS": {
    "southfork": "/absolute/path/southfork.duckdb",
    "asgard": "/absolute/path/asgard.duckdb"
  }
}
```

`F3_ANALYTICS_DB_PATH` remains the fallback path for South Fork only (including
existing eval fixtures). Missing region mappings return unavailable; there is no
cross-region fallback. Every configured snapshot must use the same schema, with
that region ID in all data tables and import_metadata. The backend checks this
before sending any question to the model. The current importer still builds South
Fork snapshots; adding another region requires preparing its snapshot separately.
For compatibility older API clients that omit region still default to South Fork;
the frontend and eval harness now send it explicitly.
