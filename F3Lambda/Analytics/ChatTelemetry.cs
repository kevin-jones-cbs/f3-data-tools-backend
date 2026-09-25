using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace F3Lambda.Analytics;

public sealed class ChatTrace
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public DateTimeOffset StartedAt { get; set; } = DateTimeOffset.UtcNow;
    public string Model { get; set; } = "";
    public string? Region { get; set; }
    public ChatRequest Request { get; set; } = new([]);
    public string Status { get; set; } = "started";
    public string? Error { get; set; }
    public long DurationMs { get; set; }
    public ChatResponse? Response { get; set; }
    public List<ChatModelCall> Calls { get; set; } = [];
    public List<QueryAttempt> QueryAttempts { get; set; } = [];
}
public sealed class ChatModelCall
{
    public JsonObject? Input { get; set; }
    public JsonObject? Output { get; set; }
    public int? HttpStatus { get; set; }
    public long DurationMs { get; set; }
}
public interface IChatTelemetry
{
    Task WriteAsync(ChatTrace trace);
}

// Single application process writer. Separate from the immutable attendance snapshot.
public sealed class DuckDbChatTelemetry(string path, string executable = "duckdb", string source = "app") : IChatTelemetry
{
    private readonly SemaphoreSlim gate = new(1);
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    public async Task WriteAsync(ChatTrace trace)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await gate.WaitAsync(timeout.Token);
        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);
            var start = new ProcessStartInfo(executable)
            {
                RedirectStandardInput = true, RedirectStandardOutput = true,
                RedirectStandardError = true, UseShellExecute = false, CreateNoWindow = true
            };
            var searchPath = Environment.GetEnvironmentVariable("PATH");
            start.Environment.Clear();
            if (searchPath != null) start.Environment["PATH"] = searchPath;
            foreach (var arg in new[] { "-no-init", "-batch", "-bail", path }) start.ArgumentList.Add(arg);
            using var process = Process.Start(start) ?? throw new IOException("Cannot start telemetry writer");
            using var registration = timeout.Token.Register(() =>
            {
                try { if (!process.HasExited) process.Kill(true); } catch (InvalidOperationException) { }
            });
            var output = process.StandardOutput.ReadToEndAsync(timeout.Token);
            var error = process.StandardError.ReadToEndAsync(timeout.Token);
            // Hex encoding keeps user content out of SQL and CLI dot-command parsing.
            var hex = Convert.ToHexString(System.Text.Encoding.UTF8.GetBytes(JsonSerializer.Serialize(trace, JsonOptions)));
            var sourceHex = Convert.ToHexString(System.Text.Encoding.UTF8.GetBytes(source));
            await process.StandardInput.WriteLineAsync((Schema + $"\nINSERT INTO chat_events SELECT decode(from_hex('{sourceHex}')), CAST(decode(from_hex('{hex}')) AS JSON);").AsMemory(), timeout.Token);
            process.StandardInput.Close();
            await process.WaitForExitAsync(timeout.Token);
            await Task.WhenAll(output, error);
            if (process.ExitCode != 0) throw new IOException("DuckDB telemetry write failed (database may be locked).");
        }
        finally { gate.Release(); }
    }

    public async Task<JsonElement[]> ListAsync(int offset, string? search, string? status, CancellationToken ct)
    {
        if (offset < 0 || offset > 100000 || (search?.Length ?? 0) > 200)
            throw new ArgumentException("Invalid paging or search input.");
        var filters = new List<string>();
        if (!string.IsNullOrWhiteSpace(search))
        {
            var hex = Convert.ToHexString(System.Text.Encoding.UTF8.GetBytes(search.ToLowerInvariant()));
            filters.Add($"contains(lower(coalesce(t.question, '') || ' ' || coalesce(t.answer, '')), decode(from_hex('{hex}')))");
        }
        if (!string.IsNullOrEmpty(status))
        {
            if (status is not ("success" or "error" or "canceled_or_timeout")) throw new ArgumentException("Invalid status.");
            filters.Add($"t.status = '{status}'");
        }
        var where = filters.Count == 0 ? "" : "WHERE " + string.Join(" AND ", filters);
        return await ReadAsync($"""
            SELECT t.request_id, t.started_at, t.model, r.region, t.status, t.source, t.conversation_id,
              t.duration_ms, left(t.question, 300) AS question, t.model_calls,
              u.input_tokens, u.output_tokens, u.cost_usd, u.usage_calls, u.cost_calls
            FROM chat_turns t LEFT JOIN (
              SELECT request_id, CAST(sum(input_tokens) AS BIGINT) AS input_tokens,
                CAST(sum(output_tokens) AS BIGINT) AS output_tokens, sum(cost_usd) AS cost_usd,
                count(input_tokens) AS usage_calls, count(cost_usd) AS cost_calls
              FROM chat_model_calls GROUP BY request_id
            ) u USING (request_id)
            LEFT JOIN (SELECT event->>'id' AS request_id, {RegionExpression} AS region FROM chat_events) r USING (request_id)
            {where} ORDER BY t.started_at DESC, t.request_id DESC LIMIT 26 OFFSET {offset}
            """, ct);
    }

    public async Task<JsonElement?> GetAsync(Guid id, CancellationToken ct)
    {
        var rows = await ReadAsync($"SELECT source, event, {RegionExpression} AS region FROM chat_events WHERE event->>'id' = '{id:D}' LIMIT 1", ct);
        return rows.Length == 0 ? null : rows[0];
    }

    private async Task<JsonElement[]> ReadAsync(string sql, CancellationToken ct)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));
        await gate.WaitAsync(timeout.Token); // Share the writer gate: no read/write process lock conflicts.
        try
        {
            if (!File.Exists(path)) return [];
            var start = new ProcessStartInfo(executable) { RedirectStandardOutput = true,
                RedirectStandardError = true, RedirectStandardInput = true, UseShellExecute = false };
            var searchPath = Environment.GetEnvironmentVariable("PATH");
            start.Environment.Clear();
            if (searchPath != null) start.Environment["PATH"] = searchPath;
            foreach (var arg in new[] { "-readonly", "-no-init", "-batch", "-bail", "-json", path, "-c", sql }) start.ArgumentList.Add(arg);
            using var process = Process.Start(start) ?? throw new IOException("Cannot start telemetry reader");
            process.StandardInput.Close();
            using var registration = timeout.Token.Register(() => {
                try { if (!process.HasExited) process.Kill(true); } catch (InvalidOperationException) { }
            });
            var output = process.StandardOutput.ReadToEndAsync(timeout.Token);
            var error = process.StandardError.ReadToEndAsync(timeout.Token);
            await Task.WhenAll(output, error);
            await process.WaitForExitAsync(timeout.Token);
            if (process.ExitCode != 0) throw new IOException("Cannot read chat logs.");
            var json = await output;
            return JsonSerializer.Deserialize<JsonElement[]>(string.IsNullOrWhiteSpace(json) ? "[]" : json)!;
        }
        finally { gate.Release(); }
    }

    private const string RegionExpression = "coalesce(event->>'region', event->'request'->>'region', CASE WHEN starts_with(json_extract_string(event, '$.calls[0].input.messages[0].content'), 'You are the South Fork F3 attendance analyst.') THEN 'southfork' ELSE NULL END)";

    private const string Schema = """
        CREATE TABLE IF NOT EXISTS chat_events(source VARCHAR, event JSON);
        CREATE VIEW IF NOT EXISTS chat_turns AS
        SELECT source, event->>'id' AS request_id,
          CAST(event->>'startedAt' AS TIMESTAMPTZ) AS started_at,
          event->>'model' AS model, event->>'status' AS status,
          event->>'error' AS error, CAST(event->>'durationMs' AS BIGINT) AS duration_ms,
          event->'request'->>'conversationId' AS conversation_id,
          event->'request'->>'visitorId' AS visitor_id,
          json_array_length(event->'request'->'messages') AS message_count,
          json_extract_string(event, '$.request.messages[#-1].content') AS question,
          event->'response'->>'answer' AS answer,
          event->'response'->'visualizations' AS results,
          event->'queryAttempts' AS query_attempts,
          json_array_length(event->'calls') AS model_calls
        FROM chat_events;
        CREATE VIEW IF NOT EXISTS chat_model_calls AS
        SELECT e.source, e.event->>'id' AS request_id, CAST(c.key AS INTEGER) + 1 AS call_number,
          e.event->>'model' AS requested_model, c.value->'output'->>'model' AS returned_model,
          c.value->'output'->>'id' AS generation_id,
          CAST(c.value->>'httpStatus' AS INTEGER) AS http_status,
          CAST(c.value->>'durationMs' AS BIGINT) AS duration_ms,
          TRY_CAST(c.value->'output'->'usage'->>'prompt_tokens' AS BIGINT) AS input_tokens,
          TRY_CAST(c.value->'output'->'usage'->>'completion_tokens' AS BIGINT) AS output_tokens,
          TRY_CAST(c.value->'output'->'usage'->>'total_tokens' AS BIGINT) AS total_tokens,
          TRY_CAST(c.value->'output'->'usage'->>'cost' AS DOUBLE) AS cost_usd,
          c.value->'output'->'usage' AS usage,
          c.value->'input' AS input, c.value->'output' AS output
        FROM chat_events e, json_each(e.event->'calls') c;
        """;
}
