using Amazon.S3;
using Amazon.S3.Model;
using F3Lambda.Analytics;
using System.Text.Json;
using Xunit;

namespace F3Lambda.Tests;

public class S3ChatTelemetryTests
{
    [Fact]
    public async Task ConcurrentTurnsHaveIndependentUtcPartitionedObjectsAndPreserveTrace()
    {
        using var s3 = new FakeS3();
        var writer = new S3ChatTelemetry(s3, "private-bucket", "/chat-logs/sandbox/");
        var traces = Enumerable.Range(0, 20).Select(_ => new ChatTrace
        {
            StartedAt = DateTimeOffset.Parse("2026-09-30T23:30:00-07:00"),
            Region = "southfork", Status = "error", Error = "Example failure",
            Request = new ChatRequest([new ChatMessage("user", "Who posted? ' 💪")])
        }).ToArray();
        await Task.WhenAll(traces.Select(writer.WriteAsync));
        Assert.Equal(20, s3.Requests.Select(r => r.Key).Distinct().Count());
        foreach (var request in s3.Requests)
        {
            Assert.Equal("private-bucket", request.BucketName);
            Assert.StartsWith("chat-logs/sandbox/2026/10/01/", request.Key);
            Assert.Equal("application/json", request.ContentType);
            Assert.Equal(ServerSideEncryptionMethod.AES256, request.ServerSideEncryptionMethod);
            var trace = JsonSerializer.Deserialize<ChatTrace>(request.ContentBody, new JsonSerializerOptions(JsonSerializerDefaults.Web))!;
            Assert.Equal("error", trace.Status);
            Assert.Equal("Who posted? ' 💪", trace.Request.Messages[0].Content);
            Assert.EndsWith(trace.Id + ".json", request.Key);
        }
    }

    [Fact]
    public async Task S3FailuresAreObservableToTheChatService()
    {
        using var s3 = new FakeS3 { Fail = true };
        await Assert.ThrowsAsync<AmazonS3Exception>(() => new S3ChatTelemetry(s3, "bucket", "logs").WriteAsync(new ChatTrace()));
    }

    private sealed class FakeS3() : AmazonS3Client(new Amazon.Runtime.AnonymousAWSCredentials(), Amazon.RegionEndpoint.USWest1)
    {
        public System.Collections.Concurrent.ConcurrentBag<PutObjectRequest> Requests { get; } = [];
        public bool Fail { get; init; }
        public override Task<PutObjectResponse> PutObjectAsync(PutObjectRequest request, CancellationToken cancellationToken = default)
        {
            Assert.True(cancellationToken.CanBeCanceled);
            if (Fail) throw new AmazonS3Exception("Unavailable");
            Requests.Add(request);
            return Task.FromResult(new PutObjectResponse());
        }
    }
}
