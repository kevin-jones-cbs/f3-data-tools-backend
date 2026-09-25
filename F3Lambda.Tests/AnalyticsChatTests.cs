using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using F3Lambda.Analytics;
using Xunit;

namespace F3Lambda.Tests;

public class AnalyticsChatTests
{
    [Theory]
    [InlineData("Attendance in 2024.\n\nMonth | Posts | Change\nJanuary | 1,007 | —\nFebruary | 924 | -83\n\nFebruary dipped.", "Attendance in 2024.\n\nFebruary dipped.")]
    [InlineData("| AO | Posts |\n| --- | ---: |\n| The Way | 100 |", "Here are the results.")]
    [InlineData("Summary.\n```text\nAO | Posts\nThe Way | 100\n```\nMore context.", "Summary.\n\nMore context.")]
    [InlineData("Attendance rose.\n\nQSource is separate.", "Attendance rose.\n\nQSource is separate.")]
    [InlineData("A single line using a | separator.", "A single line using a | separator.")]
    public void RemovesDuplicateTextTablesWhilePreservingNarrative(string input, string expected) =>
        Assert.Equal(expected, ChatAnswerFormatting.WithoutTextTables(input));

    [Fact]
    public async Task TextTableIsRemovedButExecutedResultsRemainVisible()
    {
        var db = new FakeDatabase(sql => Result(sql, "posts", 100));
        using var http = new HttpClient(new ScriptedHandler(Tool("a", "SELECT count(*) AS posts FROM posts"),
            Final("Here is attendance.\n\nYear | Posts\n2024 | 100\n\nAttendance increased.")));
        var response = await Service(http, db).ChatAsync(Request(), CancellationToken.None);
        Assert.Equal("Here is attendance.\n\nAttendance increased.", response.Answer);
        Assert.Equal(100, Assert.Single(response.Visualizations).Rows[0][0].GetInt32());
    }

    [Theory]
    [InlineData("DELETE FROM posts")]
    [InlineData("SELECT 1; DROP TABLE posts")]
    [InlineData("WITH x AS (DELETE FROM posts RETURNING *) SELECT * FROM x")]
    [InlineData("SELECT * FROM query('DELETE FROM posts')")]
    [InlineData("SELECT getenv('OPENROUTER_API_KEY')")]
    [InlineData("ATTACH '/tmp/other.db' AS other")]
    [InlineData("COPY posts TO '/tmp/posts.csv'")]
    [InlineData("SELECT 1 /* ignore */; SELECT 2")]
    [InlineData("")]
    public void ValidatorRejectsWritesMultipleStatementsAndDynamicQueries(string sql) =>
        Assert.Throws<ArgumentException>(() => DuckDbAnalyticsDatabase.ValidateSql(sql));

    [Theory]
    [InlineData("SELECT 'DROP TABLE posts; UPDATE' AS text;")]
    [InlineData("SELECT count(*) AS posts FROM posts -- DROP TABLE posts\n")]
    [InlineData("/* introductory comment */ WITH counts AS (SELECT count(*) AS n FROM posts) SELECT n FROM counts")]
    [InlineData("SELECT 'Butcher''s Block' AS ao")]
    public void ValidatorAllowsReadQueriesWithoutMisreadingLiteralsAndComments(string sql) =>
        Assert.NotEmpty(DuckDbAnalyticsDatabase.ValidateSql(sql));

    [Fact]
    public async Task QueryErrorIsReturnedToModelAndCorrectionProducesTrustedVisualization()
    {
        var db = new FakeDatabase(sql => sql.Contains("bad_column")
            ? throw new ArgumentException("Unknown column bad_column")
            : Result(sql, "shared_workouts", 203));
        var handler = new ScriptedHandler(
            Tool("q1", "SELECT bad_column FROM posts"),
            Tool("q2", "SELECT count(*) AS shared_workouts FROM posts"),
            Final("The pair shared 203 workouts."));
        using var http = new HttpClient(handler);
        var service = Service(http, db);

        var answer = await service.ChatAsync(Request(), CancellationToken.None);

        Assert.Equal(2, db.Sql.Count);
        var query = Assert.Single(answer.Queries);
        Assert.Equal(203, query.Rows[0][0].GetInt32());
        Assert.Contains("Unknown column bad_column", handler.Requests[1]);
        Assert.Contains("shared_workouts", handler.Requests[2]);
        Assert.Equal("fake/model", answer.Model);
        Assert.Equal(db.Snapshot, answer.Snapshot);
        var view = Assert.Single(answer.Visualizations);
        Assert.Equal("table", view.Kind);
        Assert.Equal("Shared Workouts", Assert.Single(view.Columns).Label);
        Assert.Equal(query.Rows, view.Rows);
        Assert.DoesNotContain("sql", JsonSerializer.Serialize(view).ToLowerInvariant());
    }

    [Fact]
    public async Task FinalVisualizationUsesLastExecutedResultNotModelSuggestedNumbers()
    {
        var db = new FakeDatabase(sql => Result(sql, "posts", sql.Contains("2025") ? 42 : 100));
        var handler = new ScriptedHandler(Tool("a", "SELECT 100 AS posts"),
            Tool("b", "SELECT 2025 AS posts"), Final("The count is 999."));
        using var http = new HttpClient(handler);
        var response = await Service(http, db).ChatAsync(Request(), CancellationToken.None);
        Assert.Equal(2, response.Queries.Count);
        Assert.Equal(42, Assert.Single(response.Visualizations).Rows[0][0].GetInt32());
        // Narrative accuracy is evaluated separately; table data never comes from prose.
    }

    [Fact]
    public async Task FailureAfterSuccessfulQueryDoesNotPresentStaleVisualization()
    {
        var db = new FakeDatabase(sql => sql.Contains("bad_column")
            ? throw new ArgumentException("Unknown column") : Result(sql, "posts", 100));
        using var http = new HttpClient(new ScriptedHandler(Tool("a", "SELECT count(*) FROM posts"),
            Tool("b", "SELECT bad_column FROM posts"), Final("I could not complete the final query.")));
        var response = await Service(http, db).ChatAsync(Request(), CancellationToken.None);
        Assert.Single(response.Queries);
        Assert.Empty(response.Visualizations);
    }

    [Fact]
    public async Task ClarificationWithoutQueriesDoesNotFabricateVisualization()
    {
        var db = new FakeDatabase(sql => throw new Exception("Should not query"));
        using var http = new HttpClient(new ScriptedHandler(Final("Which year should I compare?")));
        var response = await Service(http, db).ChatAsync(Request(), CancellationToken.None);
        Assert.Empty(response.Queries);
        Assert.Empty(response.Visualizations);
        Assert.Empty(db.Sql);
    }

    [Fact]
    public async Task DisallowedModelIsRejectedBeforeDatabaseOrProviderAccess()
    {
        var db = new FakeDatabase(sql => Result(sql, "posts", 1));
        var handler = new ScriptedHandler();
        using var http = new HttpClient(handler);
        await Assert.ThrowsAsync<ArgumentException>(() => Service(http, db).ChatAsync(
            new ChatRequest([new ChatMessage("user", "Count posts")], "unexpected/model"), CancellationToken.None));
        Assert.Equal(0, db.SnapshotReads);
        Assert.Empty(handler.Requests);
    }

    [Fact]
    public async Task FollowUpHistoryAndExplicitModelAreForwarded()
    {
        var db = new FakeDatabase(sql => Result(sql, "posts", 1));
        var handler = new ScriptedHandler(Final("Which AO?"));
        using var http = new HttpClient(handler);
        var service = new AnalyticsChatService(http, db, "test-key", "fake/model", ["fake/model", "fake/other"]);
        var result = await service.ChatAsync(new ChatRequest([
            new ChatMessage("user", "Compare 2025 pairs"),
            new ChatMessage("assistant", "I will compare shared workouts in 2025."),
            new ChatMessage("user", "Now 2024")], "fake/other"), CancellationToken.None);
        Assert.Equal("fake/other", result.Model);
        var body = JsonNode.Parse(Assert.Single(handler.Requests))!;
        Assert.Equal("fake/other", body["model"]!.GetValue<string>());
        Assert.Equal("Now 2024", body["messages"]![3]!["content"]!.GetValue<string>());
        Assert.Contains("2023-01-01", body["messages"]![0]!["content"]!.GetValue<string>());
        Assert.Contains("region = 'southfork'", body["messages"]![0]!["content"]!.GetValue<string>());
        Assert.Contains("No region filter is necessary", body["messages"]![0]!["content"]!.GetValue<string>());
    }

    [Fact]
    public async Task QueryAttemptBudgetStopsRepeatedFailures()
    {
        var db = new FakeDatabase(sql => throw new ArgumentException("bad query"));
        using var http = new HttpClient(new ScriptedHandler(Enumerable.Range(1, 6)
            .Select(i => Tool(i.ToString(), "SELECT nonexistent FROM posts")).ToArray()));
        var exception = await Assert.ThrowsAsync<ChatQueryBudgetException>(() =>
            Service(http, db).ChatAsync(Request(), CancellationToken.None));
        Assert.Contains("five attempts", exception.Message);
        Assert.Equal(5, exception.Attempts.Count);
        Assert.All(exception.Attempts, attempt => Assert.Equal("bad query", attempt.Error));
        Assert.Equal(5, db.Sql.Count);
    }

    [Fact]
    public async Task MalformedToolArgumentsCanBeCorrected()
    {
        var malformed = Tool("bad", "unused");
        malformed["tool_calls"]![0]!["function"]!["arguments"] = "{";
        var db = new FakeDatabase(sql => Result(sql, "posts", 5));
        var handler = new ScriptedHandler(malformed, Tool("ok", "SELECT 5 AS posts"), Final("Five posts."));
        using var http = new HttpClient(handler);
        var response = await Service(http, db).ChatAsync(Request(), CancellationToken.None);
        Assert.Single(db.Sql);
        Assert.Contains("error", handler.Requests[1]);
        Assert.Single(response.Visualizations);
    }

    [Fact]
    public async Task RealDuckDbBlocksExternalReadsAndLimitsRowsWhenExplicitlyConfigured()
    {
        var path = Environment.GetEnvironmentVariable("F3_ANALYTICS_TEST_DB");
        if (string.IsNullOrWhiteSpace(path)) return; // Opt-in: CI need not install DuckDB or private data.
        var database = new DuckDbAnalyticsDatabase(new LocalAnalyticsSnapshotProvider(path));
        var result = await database.QueryAsync("SELECT i FROM range(150) AS t(i) ORDER BY i", CancellationToken.None);
        Assert.Equal(100, result.Rows.Length);
        Assert.True(result.Truncated);
        Assert.Equal(99, result.Rows[^1][0].GetInt32());
        await Assert.ThrowsAsync<ArgumentException>(() => database.QueryAsync(
            "SELECT * FROM read_text('/etc/hosts')", CancellationToken.None));
        await Assert.ThrowsAsync<ArgumentException>(() => database.QueryAsync(
            "SELECT * FROM read_csv('https://example.com/data.csv')", CancellationToken.None));
        var snapshot = await database.GetSnapshotAsync(CancellationToken.None);
        Assert.Equal(64, snapshot.Sha256.Length);
        var wrongRegion = new DuckDbAnalyticsDatabase(new LocalAnalyticsSnapshotProvider(path), expectedRegion: "asgard");
        await Assert.ThrowsAsync<InvalidOperationException>(() => wrongRegion.GetSnapshotAsync(CancellationToken.None));
    }

    [Fact]
    public async Task TelemetryCapturesAllCallsUsageAndDisplayedAnswerWithoutCredentials()
    {
        var sink = new RecordingTelemetry();
        using var http = new HttpClient(new ScriptedHandler(Tool("a", "SELECT 1"), Final("One post.")));
        var request = Request() with { ConversationId = Guid.NewGuid().ToString(), VisitorId = Guid.NewGuid().ToString() };
        var service = new AnalyticsChatService(http, new FakeDatabase(sql => Result(sql, "posts", 1)),
            "test-key", "fake/model", ["fake/model"], sink);
        await service.ChatAsync(request, CancellationToken.None);
        var trace = Assert.Single(sink.Traces);
        Assert.Equal("success", trace.Status);
        Assert.Equal(request.ConversationId, trace.Request.ConversationId);
        Assert.Equal(2, trace.Calls.Count);
        Assert.All(trace.Calls, call => Assert.Equal(10, call.Output!["usage"]!["prompt_tokens"]!.GetValue<int>()));
        Assert.Equal("One post.", trace.Response!.Answer);
        Assert.Single(trace.QueryAttempts);
        Assert.DoesNotContain("test-key", JsonSerializer.Serialize(trace));
    }

    [Fact]
    public async Task TelemetryCapturesProviderFailureAndDoesNotMaskIt()
    {
        var sink = new RecordingTelemetry();
        using var http = new HttpClient(new RateLimitHandler());
        var service = new AnalyticsChatService(http, new FakeDatabase(sql => Result(sql, "posts", 1)),
            "test-key", "fake/model", ["fake/model"], sink);
        await Assert.ThrowsAsync<HttpRequestException>(() => service.ChatAsync(Request(), CancellationToken.None));
        var trace = Assert.Single(sink.Traces);
        Assert.Equal("error", trace.Status);
        Assert.Equal(429, Assert.Single(trace.Calls).HttpStatus);
        Assert.Null(trace.Calls[0].Output); // Unavailable usage must remain unknown, not zero.
    }

    [Fact]
    public async Task TelemetryFailureDoesNotFailSuccessfulChat()
    {
        using var http = new HttpClient(new ScriptedHandler(Final("Hello.")));
        var warned = false;
        var service = new AnalyticsChatService(http, new FakeDatabase(sql => Result(sql, "posts", 1)),
            "test-key", "fake/model", ["fake/model"], new RecordingTelemetry { Fail = true }, _ => warned = true);
        Assert.Equal("Hello.", (await service.ChatAsync(Request(), CancellationToken.None)).Answer);
        Assert.True(warned);
    }

    [Fact]
    public async Task RealTelemetryRoundTripsHostileTextAndConcurrentWrites()
    {
        var executable = Environment.GetEnvironmentVariable("F3_TELEMETRY_TEST_DUCKDB");
        if (string.IsNullOrEmpty(executable)) return;
        var path = Path.Combine(Path.GetTempPath(), Guid.NewGuid() + ".duckdb");
        try
        {
            var sink = new DuckDbChatTelemetry(path, executable, "test");
            var question = "O'Brien; DROP TABLE chat_events;\n.shell echo bad";
            var trace = new ChatTrace { Request = new([new("user", question)]), Status = "success",
                Calls = [new() { Output = JsonNode.Parse("{\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2,\"total_tokens\":12,\"cost\":0.001}}")!.AsObject() }] };
            await Task.WhenAll(sink.WriteAsync(trace), sink.WriteAsync(new ChatTrace { Request = trace.Request }));
            var listed = await sink.ListAsync(0, "O'Brien", "success", CancellationToken.None);
            Assert.Single(listed);
            Assert.Equal("southfork", listed[0].GetProperty("region").GetString());
            Assert.Empty(await sink.ListAsync(0, "' OR 1=1 --", null, CancellationToken.None));
            Assert.Empty(await sink.ListAsync(25, null, null, CancellationToken.None));
            var detail = await sink.GetAsync(Guid.Parse(trace.Id), CancellationToken.None);
            Assert.Equal(question, detail!.Value.GetProperty("event").GetProperty("request").GetProperty("messages")[0].GetProperty("content").GetString());
            Assert.Null(await sink.GetAsync(Guid.NewGuid(), CancellationToken.None));
            await Assert.ThrowsAsync<ArgumentException>(() => sink.ListAsync(-1, null, null, CancellationToken.None));
            var db = new DuckDbAnalyticsDatabase(new LocalAnalyticsSnapshotProvider(path), executable);
            var turns = await db.QueryAsync("SELECT question FROM chat_turns", CancellationToken.None);
            Assert.Equal(2, turns.Rows.Length);
            Assert.All(turns.Rows, row => Assert.Equal(question, row[0].GetString()));
            var usage = await db.QueryAsync("SELECT CAST(sum(input_tokens) AS BIGINT), CAST(sum(output_tokens) AS BIGINT), sum(cost_usd) FROM chat_model_calls", CancellationToken.None);
            Assert.Equal(10, usage.Rows[0][0].GetInt32());
            Assert.Equal(2, usage.Rows[0][1].GetInt32());
            Assert.Equal(0.001, usage.Rows[0][2].GetDouble(), 6);
        }
        finally { File.Delete(path); File.Delete(path + ".wal"); }
    }

    private sealed class RecordingTelemetry : IChatTelemetry
    {
        public bool Fail { get; init; }
        public List<ChatTrace> Traces { get; } = [];
        public Task WriteAsync(ChatTrace trace)
        {
            if (Fail) throw new IOException("disk full");
            Traces.Add(trace);
            return Task.CompletedTask;
        }
    }
    [Fact]
    public async Task StreamingChatExecutesToolsAndRetainsFinalUsageAndTable()
    {
        var sink = new RecordingTelemetry();
        using var http = new HttpClient(new StreamingHandler());
        var service = new AnalyticsChatService(http, new FakeDatabase(sql => Result(sql, "posts", 1)),
            "test-key", "fake/model", ["fake/model"], sink);
        var updates = new List<string>();
        var statuses = 0;
        var result = await service.ChatAsync(Request(), CancellationToken.None,
            (type, text) => { if (type == "answer") updates.Add(text); else if (type == "status") statuses++; return Task.CompletedTask; });
        Assert.Equal(1, statuses);
        Assert.Equal(new[] { "One", "One post." }, updates);
        Assert.Equal("One post.", result.Answer);
        Assert.Single(result.Visualizations);
        Assert.Single(result.Queries);
        Assert.Equal(2, sink.Traces[0].Calls.Count);
        Assert.All(sink.Traces[0].Calls, call => Assert.Equal(12, call.Output!["usage"]!["total_tokens"]!.GetValue<int>()));
    }
    private sealed class StreamingHandler : HttpMessageHandler
    {
        private int count;
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var payload = JsonNode.Parse(await request.Content!.ReadAsStringAsync(ct))!;
            Assert.True(payload["stream"]!.GetValue<bool>());
            var message = ++count == 1 ? Tool("q1", "SELECT 1") : Final("One post.");
            var frames = new List<string>();
            if (count == 1)
            {
                message["tool_calls"]![0]!["index"] = 0;
                frames.Add(new JsonObject { ["choices"] = new JsonArray(new JsonObject { ["delta"] = message, ["finish_reason"] = "tool_calls" }) }.ToJsonString());
            }
            else
            {
                foreach (var part in new[] { "One", " post." })
                    frames.Add(new JsonObject { ["choices"] = new JsonArray(new JsonObject { ["delta"] = new JsonObject { ["content"] = part }, ["finish_reason"] = part == "One" ? null : "stop" }) }.ToJsonString());
            }
            frames.Add("{\"choices\":[],\"usage\":{\"total_tokens\":12}}");
            frames.Add("[DONE]");
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(
                string.Join("", frames.Select(frame => "data: " + frame + "\n\n")), Encoding.UTF8, "text/event-stream") };
        }
    }

    private sealed class RateLimitHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.TooManyRequests));
    }

    [Fact]
    public async Task RegionFlowsIntoPromptAndTelemetryAndCannotSwitchServiceSnapshot()
    {
        var sink = new RecordingTelemetry();
        var handler = new ScriptedHandler(Final("Asgard results."));
        using var http = new HttpClient(handler);
        var service = new AnalyticsChatService(http, new FakeDatabase(sql => Result(sql, "posts", 1)),
            "test-key", "fake/model", ["fake/model"], sink, region: "asgard");
        await service.ChatAsync(Request() with { Region = "asgard" }, CancellationToken.None);
        Assert.Contains("Asgard F3 attendance analyst", handler.Requests[0]);
        Assert.DoesNotContain("South Fork", handler.Requests[0]);
        Assert.Equal("asgard", Assert.Single(sink.Traces).Region);
        await Assert.ThrowsAsync<ArgumentException>(() => service.ChatAsync(Request(), CancellationToken.None));
        Assert.Single(handler.Requests);
    }

    private static AnalyticsChatService Service(HttpClient http, IAnalyticsDatabase db) =>
        new(http, db, "test-key", "fake/model", ["fake/model"]);
    private static ChatRequest Request() => new([new ChatMessage("user", "Count posts in 2025")]);
    private static QueryResult Result(string sql, string column, int value) =>
        new(sql, [column], [[JsonSerializer.SerializeToElement(value)]], false);
    private static JsonObject Final(string text) => new() { ["role"] = "assistant", ["content"] = text };
    private static JsonObject Tool(string id, string sql) => new()
    {
        ["role"] = "assistant", ["content"] = null,
        ["tool_calls"] = new JsonArray(new JsonObject
        {
            ["id"] = id, ["type"] = "function", ["function"] = new JsonObject
            {
                ["name"] = "query_analytics",
                ["arguments"] = JsonSerializer.Serialize(new { sql })
            }
        })
    };

    private sealed class FakeDatabase(Func<string, QueryResult> execute) : IAnalyticsDatabase
    {
        public List<string> Sql { get; } = [];
        public int SnapshotReads { get; private set; }
        public SnapshotInfo Snapshot { get; } = new("2026-09-22", "2023-01-01", "2025-12-31", "fixture-hash");
        public Task<QueryResult> QueryAsync(string sql, CancellationToken cancellationToken)
        {
            Sql.Add(sql);
            return Task.FromResult(execute(sql));
        }
        public Task<SnapshotInfo> GetSnapshotAsync(CancellationToken cancellationToken)
        {
            SnapshotReads++;
            return Task.FromResult(Snapshot);
        }
    }

    private sealed class ScriptedHandler(params JsonObject[] replies) : HttpMessageHandler
    {
        public List<string> Requests { get; } = [];
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Assert.Equal("Bearer", request.Headers.Authorization?.Scheme);
            Assert.Equal("test-key", request.Headers.Authorization?.Parameter);
            Requests.Add(await request.Content!.ReadAsStringAsync(cancellationToken));
            Assert.True(Requests.Count <= replies.Length, "Unexpected extra model call");
            var response = new JsonObject { ["usage"] = new JsonObject { ["prompt_tokens"] = 10, ["completion_tokens"] = 2 }, ["choices"] = new JsonArray(new JsonObject
                { ["message"] = replies[Requests.Count - 1].DeepClone() }) };
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(response.ToJsonString(), Encoding.UTF8, "application/json")
            };
        }
    }
}
