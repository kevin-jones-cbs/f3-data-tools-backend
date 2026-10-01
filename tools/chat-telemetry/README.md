# Local chat telemetry

Every accepted chat turn is logged automatically to `data/chat-telemetry.duckdb`
(relative to this directory). This is separate from attendance snapshots: refreshes
and frozen eval fingerprints are unaffected. No API keys or HTTP authorization
headers are recorded. Inputs and outputs do contain the full conversation, system
prompt, SQL, attendance results and model replies. Keep this ignored database local.
The model's read-only attendance connection cannot access this file.

Optional settings in `F3Lambda/Secrets/chat.local.json` (restart backend):

```json
{
  "F3_CHAT_LOG_DB_PATH": "/absolute/path/chat-telemetry.duckdb",
  "F3_CHAT_LOG_SOURCE": "app"
}
```

For an eval backend, set `F3_CHAT_LOG_SOURCE=eval` and use a separate
`F3_CHAT_LOG_DB_PATH` (e.g. `tools/chat-telemetry/data/evals.duckdb`). Use one
backend process per telemetry file. Reads can conflict with the brief writer lock;
close interactive DuckDB sessions before chatting. Writes are serialized and bounded
to five seconds. A write failure is warned in backend logs and does not fail the chat;
this is best-effort logging, not a durable audit system. Crashes before completion
can lose the current trace. Request validation failures and busy-gate rejections
happen before tracing and are not included.

## Query it

From this directory:

```sh
duckdb -readonly data/chat-telemetry.duckdb
```

`chat_turns`: one row per question, request/conversation/visitor IDs, UTC start time,
model, status, duration, question, displayed answer/results and SQL attempts.
`chat_model_calls`: one row per OpenRouter call, full request/response JSON,
generation ID, HTTP status, duration, provider-reported token counts and cost.
`chat_events`: underlying JSON records, including complete chat request and response.

Tokens include all calls in the tool loop. Missing usage/cost is NULL, not zero.
`usage` preserves cache/reasoning breakdowns when returned by the provider. Cost is
provider-reported only; there is no estimate or backfill for old chats. Never sum
per-turn duration across a join with multiple calls (it would multiply latency).

```sql
-- Daily engagement: anonymous browsers, conversations, questions and outcomes.
SELECT date_trunc('day', started_at) AS day,
       count(*) AS questions, count(DISTINCT visitor_id) AS browsers,
       count(DISTINCT conversation_id) AS conversations,
       count(*) FILTER (WHERE status = 'success') AS successful,
       round(avg(duration_ms) / 1000, 2) AS average_seconds
FROM chat_turns WHERE source = 'app' GROUP BY 1 ORDER BY 1 DESC;

-- Usage/cost by model; coverage counts expose missing provider usage.
SELECT requested_model, count(*) AS calls,
       count(total_tokens) AS calls_with_token_usage,
       sum(input_tokens) AS input_tokens, sum(output_tokens) AS output_tokens,
       count(cost_usd) AS calls_with_cost, sum(cost_usd) AS reported_cost_usd
FROM chat_model_calls GROUP BY 1;

-- Recent questions, responses and errors.
SELECT started_at, request_id, model, question, answer, status, error
FROM chat_turns ORDER BY started_at DESC LIMIT 20;

-- Follow-up depth per conversation (attempts include failed questions).
SELECT conversation_id, count(*) AS turns, min(started_at) AS first_question,
       max(started_at) AS last_question
FROM chat_turns WHERE source = 'app' AND conversation_id IS NOT NULL
GROUP BY 1 ORDER BY turns DESC;

-- Inspect all model inputs/outputs for one request, in order.
SELECT call_number, http_status, input_tokens, output_tokens, input, output
FROM chat_model_calls WHERE request_id = 'paste-request-id' ORDER BY call_number;
```

The frontend persists a random visitor UUID in localStorage and creates a fresh
conversation UUID on page load or New chat. These are browser identifiers, not
verified user accounts: different devices count separately, clearing storage resets
them, and disabled storage uses an ephemeral ID. Older clients/eval requests may
omit IDs. No IP address or identity fingerprinting is collected.

This DuckDB writer is local only. Sandbox Lambda uses `S3ChatTelemetry` to save
one JSON object per turn; see [hosted setup](../../deploy/chat/README.md#hosted-chat-telemetry).
No automatic retention cleanup is enabled; delete the local database when no
longer needed, with the backend stopped.

## Local admin page

Open **http://localhost:5090/admin/chats** directly (or your local frontend port);
there is no link from member chat.
Enter the shared admin password configured in `F3_CHAT_ADMIN_PASSWORD` in the
backend-only `Secrets/chat.local.json` (or environment). Missing configuration
disables access. Both list and detail endpoints verify the password. The browser
keeps it only in page memory; Lock admin, navigation, or reload clears it.
The page shows 25 turns per page, newest first, with question/answer search and a
status filter. Select a turn for the conversation sent to the model, the displayed
answer and tables, SQL attempts, and expandable model-call inputs/outputs/usage.
Choose **Sandbox (S3)** (the default) to read hosted chats or **Local chats** for
the local DuckDB. Sandbox reads use the backend's `kevin-personal` AWS profile;
no manual sync or AWS credentials in the browser are needed. The default location
is `s3://f3-data-tools-config-311293999880/chat-logs/sandbox/` in `us-west-1`.
Optional local configuration overrides are `F3_CHAT_LOG_AWS_PROFILE`,
`F3_CHAT_LOG_AWS_REGION`, `F3_CHAT_LOG_S3_BUCKET`, and `F3_CHAT_LOG_S3_PREFIX`.

Sandbox defaults to the last seven UTC dates; choose up to 31 days. Summary cards
show matching turns, anonymous browsers, conversations, errors/interrupted turns,
and reported cost, with daily question counts. S3 listings refresh at most once
per minute and previously downloaded immutable traces are reused. Each window is
limited to 5,000 objects or 32 MiB of serialized traces, with a visible warning
when results are partial. Narrow the date range for a larger archive.

List token totals are provider-reported sums; incomplete usage is labeled. Costs
and missing usage remain unknown rather than estimated.

`GET /admin/chats?offset=0&search=...&status=...` and
`GET /admin/chats/{request-uuid}` are read-only and share the telemetry writer lock.
They require a loopback connection/host and reject browser origins other than the
local frontend. This shared-password gate remains local-only; do not proxy these
endpoints publicly. Responses use `Cache-Control: no-store`. No model requests are
made by this page. Local chats are read from the configured F3_CHAT_LOG_DB_PATH
file; eval logs stored in a different file are not merged into this list.
