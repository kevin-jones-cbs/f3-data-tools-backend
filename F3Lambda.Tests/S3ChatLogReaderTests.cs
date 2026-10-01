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
    public async Task PagesSearchAndSummaryReuseDownloadsAndPreserveMissingUsage()
    {
        using var s3 = new FakeS3();
        for (int i = 0; i < 27; i++) s3.Add(new ChatTrace
        {
            StartedAt = new DateTimeOffset(2026, 9, 30, 0, i, 0, TimeSpan.Zero), Status = i == 26 ? "error" : "success",
            Request = new ChatRequest([new("user", "Question " + i)], ConversationId: "conversation", VisitorId: "browser"),
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

    private sealed class FakeS3() : AmazonS3Client(new Amazon.Runtime.AnonymousAWSCredentials(), Amazon.RegionEndpoint.USWest1)
    {
        private readonly Dictionary<string, string> objects = [];
        public int Lists, Downloads;
        public bool Paginate;
        public long? ObjectSize;
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
