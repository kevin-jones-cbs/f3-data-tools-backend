using Amazon.S3;
using Amazon.S3.Model;
using F3Lambda.Analytics;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Xunit;

namespace F3Lambda.Tests;

public class S3ChatLogReaderTests
{
    private static readonly DateOnly Day = new(2026, 9, 30);
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);
    private static JsonElement Json(object value) => JsonSerializer.SerializeToElement(value, Options);

    [Fact]
    public async Task DetailLoadsSelectedWindowWithoutPriorListOnThisInstance()
    {
        using var s3 = new FakeS3();
        var trace = new ChatTrace { StartedAt = new DateTimeOffset(2026, 9, 30, 1, 0, 0, TimeSpan.Zero),
            Region = "goldrush", Request = new([new("user", "Gold Rush question")], Region: "goldrush") };
        s3.Add(trace);
        var reader = new S3ChatLogReader(s3, "bucket", "logs");
        var detail = Json((await reader.GetAsync(Guid.Parse(trace.Id), default, Day, Day))!);
        Assert.Equal("goldrush", detail.GetProperty("region").GetString());
        Assert.Equal(trace.Id, detail.GetProperty("event").GetProperty("id").GetString());
        await Assert.ThrowsAsync<ArgumentException>(() => reader.GetAsync(Guid.Parse(trace.Id), default, Day.AddDays(-31), Day));
    }

    [Fact]
    public async Task PagesSearchAndSummaryReuseDownloadsAndPreserveMissingUsage()
    {
        using var s3 = new FakeS3();
        for (int i = 0; i < 27; i++) s3.Add(new ChatTrace
        {
            StartedAt = new DateTimeOffset(2026, 9, 30, 0, i, 0, TimeSpan.Zero), Status = i == 26 ? "error" : "success",
            Request = new ChatRequest([new("user", "Question " + i)], ConversationId: "conversation-" + i, VisitorId: "browser"),
            Calls = i == 26 ? [new() { Output = JsonNode.Parse("{\"usage\":{\"cost\":0.02,\"prompt_tokens\":10,\"completion_tokens\":4}}")!.AsObject() }] : [new()]
        });
        var reader = new S3ChatLogReader(s3, "bucket", "logs");
        var page = Json(await reader.ListAsync(Day, Day, 0, null, null, default));
        Assert.Equal(25, page.GetProperty("items").GetArrayLength());
        Assert.True(page.GetProperty("hasMore").GetBoolean());
        Assert.Equal("Question 26", page.GetProperty("items")[0].GetProperty("question").GetString());
        Assert.Equal(JsonValueKind.Null, page.GetProperty("items")[1].GetProperty("input_tokens").ValueKind);
        Assert.Equal(27, page.GetProperty("summary").GetProperty("turns").GetInt32());
        Assert.Equal(1, page.GetProperty("summary").GetProperty("browsers").GetInt32());
        Assert.Equal(1, page.GetProperty("summary").GetProperty("errors").GetInt32());
        Assert.Equal(0.02m, page.GetProperty("summary").GetProperty("cost").GetDecimal());
        var next = Json(await reader.ListAsync(Day, Day, 25, null, null, default));
        Assert.Equal(2, next.GetProperty("items").GetArrayLength());
        var filtered = Json(await reader.ListAsync(Day, Day, 0, "QUESTION 26", "error", default));
        Assert.Equal(1, filtered.GetProperty("summary").GetProperty("turns").GetInt32());
        Assert.Equal(27, s3.Downloads);
        Assert.Equal(1, s3.Lists);
        Assert.NotNull(await reader.GetAsync(Guid.Parse(page.GetProperty("items")[0].GetProperty("request_id").GetString()!), default));
        Assert.Null(await reader.GetAsync(Guid.NewGuid(), default));
    }

    [Fact]
    public async Task DateWindowsAreBoundedAndS3PaginationIsFollowed()
    {
        using var s3 = new FakeS3 { Paginate = true };
        s3.Add(new ChatTrace { StartedAt = new DateTimeOffset(2026, 9, 30, 1, 0, 0, TimeSpan.Zero) });
        s3.Add(new ChatTrace { StartedAt = new DateTimeOffset(2026, 9, 30, 2, 0, 0, TimeSpan.Zero) });
        var reader = new S3ChatLogReader(s3, "bucket", "logs");
        await Assert.ThrowsAsync<ArgumentException>(() => reader.ListAsync(Day.AddDays(-31), Day, 0, null, null, default));
        var page = Json(await reader.ListAsync(Day, Day, 0, null, null, default));
        Assert.Equal(2, s3.Lists);
        Assert.Equal(2, page.GetProperty("summary").GetProperty("turns").GetInt32());
    }

    [Fact]
    public async Task OversizedWindowReportsPartialWithoutDownloadingOverBudget()
    {
        using var s3 = new FakeS3 { ObjectSize = 20 * 1024 * 1024 };
        s3.Add(new ChatTrace { StartedAt = new DateTimeOffset(2026, 9, 30, 1, 0, 0, TimeSpan.Zero) });
        s3.Add(new ChatTrace { StartedAt = new DateTimeOffset(2026, 9, 30, 2, 0, 0, TimeSpan.Zero) });
        var reader = new S3ChatLogReader(s3, "bucket", "logs");
        var page = Json(await reader.ListAsync(Day, Day, 0, null, null, default));
        Assert.True(page.GetProperty("truncated").GetBoolean());
        Assert.Equal(1, s3.Downloads);
    }

    [Fact]
    public async Task OpeningEarlyTurnLoadsLaterConversationTurnsOutsideDateFilter()
    {
        using var s3 = new FakeS3();
        var conversation = Guid.NewGuid().ToString();
        var first = new ChatTrace { StartedAt = new DateTimeOffset(2026, 9, 30, 1, 0, 0, TimeSpan.Zero),
            Request = new ChatRequest([new("user", "First")], ConversationId: conversation, VisitorId: "browser") };
        var later = new ChatTrace { StartedAt = first.StartedAt.AddDays(2),
            Request = new ChatRequest([new("user", "Later")], ConversationId: conversation, VisitorId: "browser") };
        var differentBrowser = new ChatTrace { StartedAt = first.StartedAt.AddDays(1),
            Request = new ChatRequest([new("user", "Unrelated")], ConversationId: conversation, VisitorId: "other") };
        foreach (var trace in new[] { first, later, differentBrowser }) { s3.Add(trace); s3.AddIndex(trace); }
        var reader = new S3ChatLogReader(s3, "bucket", "logs");
        await reader.ListAsync(Day, Day, 0, null, null, default);
        var record = Json((await reader.GetAsync(Guid.Parse(first.Id), default))!);
        var turns = record.GetProperty("conversation").GetProperty("turns");
        Assert.Equal(2, turns.GetArrayLength());
        Assert.Equal(first.Id, turns[0].GetProperty("id").GetString());
        Assert.Equal(later.Id, turns[1].GetProperty("id").GetString());
        Assert.True(record.GetProperty("conversation").GetProperty("indexed").GetBoolean());
    }

    [Fact]
    public async Task ConversationGroupingPrecedesSearchAndAggregatesAllMatchingConversationTurns()
    {
        using var s3 = new FakeS3();
        var start = new DateTimeOffset(2026, 9, 30, 1, 0, 0, TimeSpan.Zero);
        var first = new ChatTrace { StartedAt = start, Status = "success", DurationMs = 100,
            Request = new([new("user", "Opening question")], ConversationId: "shared", VisitorId: "one") };
        var later = new ChatTrace { StartedAt = start.AddMinutes(1), Status = "error", DurationMs = 200,
            Request = new([new("user", "Opening question"), new("assistant", "Answer"), new("user", "Searchable followup")], ConversationId: "shared", VisitorId: "one") };
        s3.Add(first); s3.Add(later);
        s3.Add(new ChatTrace { StartedAt = start, Request = first.Request with { VisitorId = "two" } });
        s3.Add(new ChatTrace { StartedAt = start, Request = first.Request with { Region = "rubicon" } });
        s3.Add(new ChatTrace { StartedAt = start }); s3.Add(new ChatTrace { StartedAt = start });
        var reader = new S3ChatLogReader(s3, "bucket", "logs");
        var all = Json(await reader.ListAsync(Day, Day, 0, null, null, default));
        Assert.Equal(5, all.GetProperty("items").GetArrayLength());
        Assert.Equal(5, all.GetProperty("summary").GetProperty("conversations").GetInt32());
        var filtered = Json(await reader.ListAsync(Day, Day, 0, "Searchable", "error", default));
        var item = Assert.Single(filtered.GetProperty("items").EnumerateArray());
        Assert.Equal(first.Id, item.GetProperty("request_id").GetString());
        Assert.Equal("Opening question", item.GetProperty("question").GetString());
        Assert.Equal(2, item.GetProperty("turn_count").GetInt32());
        Assert.Equal(300, item.GetProperty("duration_ms").GetInt64());
        Assert.Equal("error", item.GetProperty("status").GetString());
        Assert.Equal(later.StartedAt, item.GetProperty("started_at").GetDateTimeOffset());
        Assert.Equal(2, filtered.GetProperty("summary").GetProperty("turns").GetInt32());
    }

    private sealed class FakeS3() : AmazonS3Client(new Amazon.Runtime.AnonymousAWSCredentials(), Amazon.RegionEndpoint.USWest1)
    {
        private readonly Dictionary<string, string> objects = [];
        public int Lists, Downloads;
        public bool Paginate;
        public long? ObjectSize;
        public void AddIndex(ChatTrace trace) => objects.Add($"logs/conversations/{trace.Request.ConversationId}/{trace.StartedAt:yyyy/MM/dd}/{trace.Id}.json", "{}");
        public void Add(ChatTrace trace) => objects.Add($"logs/{trace.StartedAt:yyyy/MM/dd}/{trace.Id}.json", JsonSerializer.Serialize(trace, Options));
        public override Task<ListObjectsV2Response> ListObjectsV2Async(ListObjectsV2Request request, CancellationToken cancellationToken = default)
        {
            Lists++;
            var entries = objects.Where(k => k.Key.StartsWith(request.Prefix)).Select(k => new S3Object { Key = k.Key, Size = ObjectSize ?? Encoding.UTF8.GetByteCount(k.Value) }).ToList();
            return Task.FromResult(new ListObjectsV2Response
            {
                S3Objects = Paginate ? entries.Skip(request.ContinuationToken == null ? 0 : 1).Take(1).ToList() : entries,
                IsTruncated = Paginate && request.ContinuationToken == null,
                NextContinuationToken = Paginate && request.ContinuationToken == null ? "next" : null
            });
        }
        public override Task<GetObjectResponse> GetObjectAsync(string bucket, string key, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref Downloads);
            return Task.FromResult(new GetObjectResponse { ResponseStream = new MemoryStream(Encoding.UTF8.GetBytes(objects[key])) });
        }
    }
}
