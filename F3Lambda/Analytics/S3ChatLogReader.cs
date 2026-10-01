using Amazon.S3;
using Amazon.S3.Model;
using System.Text.Json;

namespace F3Lambda.Analytics;

// Local-admin only. Credentials and complete traces stay in the local API process.
public sealed class S3ChatLogReader(IAmazonS3 s3, string bucket, string prefix = "chats") : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly SemaphoreSlim gate = new(1);
    private readonly Dictionary<string, ChatTrace> cache = [];
    private DateOnly? cachedFrom, cachedTo;
    private DateTimeOffset refreshedAt;
    private bool truncated;
    private const int MaxObjects = 5000;

    public async Task<object> ListAsync(DateOnly from, DateOnly to, int offset, string? search, string? status, CancellationToken ct)
    {
        if (from.Year < 2000 || to < from || to.DayNumber - from.DayNumber > 30 || offset < 0 || offset > 100000 || (search?.Length ?? 0) > 200)
            throw new ArgumentException("Choose up to 31 days and a valid page/search.");
        if (!string.IsNullOrEmpty(status) && status is not ("success" or "error" or "canceled_or_timeout"))
            throw new ArgumentException("Invalid status.");
        await gate.WaitAsync(ct);
        try
        {
            if (cachedFrom != from || cachedTo != to || DateTimeOffset.UtcNow - refreshedAt > TimeSpan.FromSeconds(60))
                await RefreshAsync(from, to, ct);
            var traces = cache.Values.Where(t => DateOnly.FromDateTime(t.StartedAt.UtcDateTime) >= from && DateOnly.FromDateTime(t.StartedAt.UtcDateTime) <= to)
                .Where(t => string.IsNullOrEmpty(status) || t.Status == status)
                .Where(t => string.IsNullOrWhiteSpace(search) || Question(t).Contains(search, StringComparison.OrdinalIgnoreCase) ||
                    (t.Response?.Answer?.Contains(search, StringComparison.OrdinalIgnoreCase) ?? false))
                .OrderByDescending(t => t.StartedAt).ThenByDescending(t => t.Id).ToArray();
            var usage = traces.SelectMany(t => t.Calls).Select(c => c.Output?["usage"]).ToArray();
            return new
            {
                items = traces.Skip(offset).Take(25).Select(Summary).ToArray(), hasMore = traces.Length > offset + 25,
                refreshedAt, truncated,
                summary = new
                {
                    turns = traces.Length,
                    browsers = traces.Select(t => t.Request.VisitorId).Where(s => !string.IsNullOrEmpty(s)).Distinct().Count(),
                    conversations = traces.Select(t => t.Request.ConversationId).Where(s => !string.IsNullOrEmpty(s)).Distinct().Count(),
                    errors = traces.Count(t => t.Status != "success"),
                    cost = usage.Sum(u => Number(u, "cost") ?? 0),
                    costCalls = usage.Count(u => Number(u, "cost") != null), modelCalls = usage.Length,
                    days = traces.GroupBy(t => t.StartedAt.UtcDateTime.ToString("yyyy-MM-dd"))
                        .OrderByDescending(g => g.Key).Select(g => new { date = g.Key, turns = g.Count() }).ToArray()
                }
            };
        }
        finally { gate.Release(); }
    }

    public async Task<object?> GetAsync(Guid id, CancellationToken ct)
    {
        await gate.WaitAsync(ct);
        try
        {
            var trace = cache.Values.FirstOrDefault(t => Guid.TryParse(t.Id, out var traceId) && traceId == id);
            return trace == null ? null : new { source = "sandbox", region = trace.Region ?? trace.Request.Region, @event = trace };
        }
        finally { gate.Release(); }
    }

    private async Task RefreshAsync(DateOnly from, DateOnly to, CancellationToken ct)
    {
        var keys = new Dictionary<string, long>();
        long bytes = 0;
        bool limited = false;
        for (var day = to; day >= from && !limited; day = day.AddDays(-1))
        {
            string? continuation = null;
            do
            {
                var page = await s3.ListObjectsV2Async(new ListObjectsV2Request
                {
                    BucketName = bucket, Prefix = $"{prefix.Trim('/')}/{day:yyyy/MM/dd}/",
                    ContinuationToken = continuation, MaxKeys = Math.Min(1000, MaxObjects + 1 - keys.Count)
                }, ct);
                foreach (var entry in page.S3Objects ?? [])
                {
                    if (!entry.Key.EndsWith(".json", StringComparison.Ordinal)) continue;
                    if (keys.Count == MaxObjects || bytes + (entry.Size ?? 0) > 32 * 1024 * 1024) { limited = true; break; }
                    keys.Add(entry.Key, entry.Size ?? 0);
                    bytes += entry.Size ?? 0;
                }
                continuation = page.IsTruncated == true ? page.NextContinuationToken : null;
            } while (continuation != null && !limited);
        }
        // Build a replacement so a failed refresh never publishes partial results.
        var replacement = new System.Collections.Concurrent.ConcurrentDictionary<string, ChatTrace>();
        await Parallel.ForEachAsync(keys.Keys, new ParallelOptions { MaxDegreeOfParallelism = 8, CancellationToken = ct }, async (key, token) =>
        {
            if (cache.TryGetValue(key, out var existing)) { replacement[key] = existing; return; }
            using var response = await s3.GetObjectAsync(bucket, key, token);
            var trace = await JsonSerializer.DeserializeAsync<ChatTrace>(response.ResponseStream, JsonOptions, token)
                ?? throw new IOException("Empty chat log.");
            replacement[key] = trace;
        });
        cache.Clear();
        foreach (var entry in replacement) cache.Add(entry.Key, entry.Value);
        cachedFrom = from; cachedTo = to; refreshedAt = DateTimeOffset.UtcNow; truncated = limited;
    }

    public void Dispose() { s3.Dispose(); gate.Dispose(); }

    private static string Question(ChatTrace trace) => trace.Request.Messages.LastOrDefault(m => m.Role == "user")?.Content ?? "";
    private static decimal? Number(System.Text.Json.Nodes.JsonNode? node, string key) =>
        decimal.TryParse(node?[key]?.ToString(), System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var value) ? value : null;
    private static object Summary(ChatTrace trace)
    {
        var usage = trace.Calls.Select(c => c.Output?["usage"]).ToArray();
        var question = Question(trace);
        return new
        {
            request_id = trace.Id, started_at = trace.StartedAt, model = trace.Model, region = trace.Region ?? trace.Request.Region,
            status = trace.Status, source = "sandbox", conversation_id = trace.Request.ConversationId,
            duration_ms = trace.DurationMs, question = question[..Math.Min(300, question.Length)], model_calls = trace.Calls.Count,
            input_tokens = usage.Any(u => Number(u, "prompt_tokens") != null) ? usage.Sum(u => Number(u, "prompt_tokens") ?? 0) : (decimal?)null,
            output_tokens = usage.Any(u => Number(u, "completion_tokens") != null) ? usage.Sum(u => Number(u, "completion_tokens") ?? 0) : (decimal?)null,
            cost_usd = usage.Any(u => Number(u, "cost") != null) ? usage.Sum(u => Number(u, "cost") ?? 0) : (decimal?)null,
            usage_calls = usage.Count(u => Number(u, "prompt_tokens") != null), cost_calls = usage.Count(u => Number(u, "cost") != null)
        };
    }
}
