# South Fork chat evaluations

A small Python standard-library harness for comparing OpenRouter models through
the actual chat backend. Requires Python 3 and the installed `duckdb` CLI.
No key is needed to verify the reference SQL.

## Stable data and expected results

`cases.json` contains 11 explicit historical questions, their conversation context,
and reviewed reference SQL. `expected.json` contains their fixed result tables.
`manifest.json` fingerprints the logical rows of each fixture table. Attendance is
restricted to **2023-01-01 through 2025-12-31**. This excludes new attendance; the
fingerprint also catches corrections to old rows. Current-year or `current_date`
queries are deliberately absent.

Cases cover FNG AO rankings, pairs of PAX (including a year-changing follow-up),
fastest 100-post milestone blocks, Peacock's attendance/Qs and AO choices,
monthly attendance/workout counts, Q leaders, QSource, and a year outside fixture
coverage. Milestone numbering explicitly restarts in 2023, so those results are
not lifetime milestones. A fixture has no 2020 rows; this is not evidence that no
workouts took place in 2020.

The existing fixture is under `data/southfork-2023-2025.duckdb`, ignored by Git.
On another machine, reconstruct it from a matching source snapshot:

```sh
cd f3-data-tools-backend/tools/chat-evals
python3 eval.py freeze --source ../southfork-duckdb/data/southfork.duckdb
python3 eval.py verify
python3 -m unittest discover -s . -p 'test_*.py'
```

`freeze` refuses to overwrite an existing fixture and rejects source data that
does not reproduce the committed fingerprint. Preserve/share the ignored original
fixture privately if the live snapshot later changes. It does not refresh Sheets.
Ancillary tables are frozen too, but only attendance tables are date-scoped; cases
do not query roster or schedule history. Snapshot import time is provenance,
not the date range covered by the fixture.

There is intentionally **no automatic golden-update command**. When deliberately
changing cases or datasets, review SQL and results, version the manifest and
expected tables together, and retain the prior fixture for older comparisons.
Generated reports and database contents are ignored; the small expected tables
contain the PAX names needed to inspect ranking accuracy.

## Compare models

Start a separate local backend pointed at the exact fixture. Set these variables
in the **backend** process, using an absolute path:

```sh
export F3_ANALYTICS_DB_PATH='/absolute/path/to/tools/chat-evals/data/southfork-2023-2025.duckdb'
export F3_CHAT_LOG_SOURCE='eval'
export F3_CHAT_LOG_DB_PATH='/absolute/path/to/tools/chat-telemetry/data/evals.duckdb'
export OPENROUTER_MODEL='openai/gpt-5.6-luna'
export OPENROUTER_ALLOWED_MODELS='openai/gpt-5.6-luna,deepseek/deepseek-v4-flash-0731'
```

Keep the API key in `F3Lambda/Secrets/chat.local.json`; the backend reads it there.
Start this separate server from `f3-data-tools-backend` (after building):

```sh
dotnet run --project F3Lambda.LocalApi --no-build --no-launch-profile --urls http://localhost:5056
```

Keep the ordinary app backend on 5055. The default model
is always permitted; additional model overrides must be in the comma-separated
allowlist. The API key never goes in the frontend or harness. Run the harness
from this directory:

```sh
python3 eval.py run --url http://localhost:5056/chat \
  --model openai/gpt-5.6-luna \
  --model deepseek/deepseek-v4-flash-0731 \
  --repeat 2 \
  --report reports/comparison.json
```

The backend currently caps query results at 100 rows, query execution at 10
seconds, and the model tool loop at five attempts. These cases return at most 25
rows.

Use `--case pairs_2025` (repeatable) for a subset, `--url` to override
`http://localhost:5055/chat`, and `--timeout` for the per-request timeout in seconds.
All fixture/baseline checks run before paid model requests. The harness invokes
models sequentially. Eleven cases × two models × two repeats means 44 chats;
each chat can make multiple model calls. Credentials are read only by the backend.

The report includes separate data/presentation counts, prose failures and pending reviews,
error count, median wall-clock latency,
SQL/result/answer responses, expected values, and a human-review checklist.
HTTP failures retain their status and response body, including failed query attempts
when the backend exhausts its query budget. These developer diagnostics are stored
only in the eval report, not shown in member chat.
Model runs exit 1 on any automated failure, 0 when automated checks pass;
setup/baseline failures exit 2. Exit 0 does not mean prose has been fully verified.

## What is scored

The **last executed query's** returned rows must exactly match the expected table,
including column position, tie-breaking, and row order. Column aliases are ignored.
The user-facing `visualizations` table must also contain the expected values,
so a successful earlier query followed by a failed final query cannot pass.
Prompts explicitly request a table shape; equivalent SQL is welcome. Truncated
results, missing queries, and empty answers fail. Other formatting choices (extra
columns, split result tables, or date-format variations) may cause a false negative;
inspect the recorded response before judging a model. The harness does not execute
model-written SQL itself.

Follow-up cases supply fixed conversation context to isolate context handling;
they are not an end-to-end evaluation of a dynamically generated earlier answer.
Scoring version 2 separates three dimensions:

- **Data:** exact query and displayed table values, as described above.
- **Presentation:** deterministic checks for exposed SQL, raw Markdown, duplicated
  text tables, and excessive narrative. These assess the returned answer string;
  visual layout remains covered by the separate frontend smoke test.
- **Prose:** conservative checks against reference facts catch specific contradictions.
  Unchecked meaning remains `needs_review`, including when no known mistake is found.
  This is deliberately not labeled “prose passed.” Review scope, unsupported causes,
  personal inferences, statistics, and whether the narrative actually answers the question.

`passed` now means **all automated checks passed**, not fully reviewed answer quality.
Do not compare that field directly with version 1's table-only score. Use
`summary.data_passed` for table accuracy across versions. Reports retain individual
issues so failures can be inspected. This is a quality benchmark, not a SQL-security
suite or a general factuality judge.

Re-score an older report without making paid calls:

```sh
python3 eval.py rescore --input reports/old.json --report reports/old-rescored.json
```

Re-scoring tests the new checks against saved answers; it does not make old runs
with different prompts comparable. For model selection run both models again
against the same backend prompt and fixture. Nemotron is excluded from the current
comparison commands; historical reports can be retained.

The endpoint is expected to return `{answer, model, queries, snapshot, visualizations}`, where each
query has `{sql, columns, rows, truncated}`. The local fixture is logically fingerprinted before requests, and every response
must attest the same database file via `snapshot.sha256` and the requested model
via `model`. Start the server with this exact fixture file. The complete returned
snapshot metadata is preserved in the report.
