using Amazon.S3;
using Amazon.S3.Model;
using System.Globalization;
using System.Text.Json;

namespace F3Lambda.Analytics;

// One immutable object per turn: no shared database or read/modify/write cycle.
public sealed class S3ChatTelemetry(IAmazonS3 s3, string bucket, string prefix) : IChatTelemetry
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public async Task WriteAsync(ChatTrace trace)
    {
        if (string.IsNullOrWhiteSpace(bucket)) throw new ArgumentException("A chat log bucket is required.");
        if (!Guid.TryParse(trace.Id, out var id)) throw new ArgumentException("A chat trace ID must be a UUID.");
        // Independent of the HTTP cancellation token so disconnected/failed requests are recorded.
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var date = trace.StartedAt.UtcDateTime.ToString("yyyy/MM/dd", CultureInfo.InvariantCulture);
        var keyPrefix = prefix.Trim('/');
        var key = $"{(keyPrefix.Length == 0 ? "" : keyPrefix + "/")}{date}/{id:D}.json";
        await s3.PutObjectAsync(new PutObjectRequest
        {
            BucketName = bucket,
            Key = key,
            ContentType = "application/json",
            ContentBody = JsonSerializer.Serialize(trace, JsonOptions),
            ServerSideEncryptionMethod = ServerSideEncryptionMethod.AES256
        }, timeout.Token);
        if (Guid.TryParse(trace.Request.ConversationId, out var conversationId))
        {
            // A small independent pointer allows complete conversation reads across date partitions.
            await s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucket,
                Key = $"{(keyPrefix.Length == 0 ? "" : keyPrefix + "/")}conversations/{conversationId:D}/{date}/{id:D}.json",
                ContentType = "application/json",
                ContentBody = JsonSerializer.Serialize(new { key }, JsonOptions),
                ServerSideEncryptionMethod = ServerSideEncryptionMethod.AES256
            }, timeout.Token);
        }
    }
}
