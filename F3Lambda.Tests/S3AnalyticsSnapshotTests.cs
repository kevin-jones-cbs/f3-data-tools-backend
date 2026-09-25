using Amazon.S3;
using Amazon.S3.Model;
using F3Lambda.Analytics;
using System.Net;
using System.Text;
using Xunit;

namespace F3Lambda.Tests;

public class S3AnalyticsSnapshotTests : IDisposable
{
    private readonly string directory = Directory.CreateTempSubdirectory("snapshot-tests-").FullName;
    private readonly FakeS3 s3 = new();
    private readonly Clock clock = new();
    private S3AnalyticsSnapshotProvider Provider(Func<string, CancellationToken, Task>? validate = null) =>
        new(s3, "bucket", "southfork.duckdb", directory, validate ?? ((_, _) => Task.CompletedTask), clock: clock);

    [Fact]
    public async Task ConcurrentColdRequestsDownloadOnceAndWarmRequestsSkipS3()
    {
        var provider = Provider();
        var paths = await Task.WhenAll(Enumerable.Range(0, 12).Select(_ => provider.GetLocalPathAsync(default)));
        Assert.Single(paths.Distinct());
        Assert.Equal("first", await File.ReadAllTextAsync(paths[0]));
        Assert.Equal(1, s3.Downloads);
        Assert.Equal(1, s3.Heads);
        clock.Advance();
        Assert.Equal(paths[0], await provider.GetLocalPathAsync(default));
        Assert.Equal(2, s3.Heads);
        Assert.Equal(1, s3.Downloads);
    }

    [Fact]
    public async Task ChangedObjectPublishesNewFileAndPinnedAnswerKeepsOldVersion()
    {
        var provider = Provider();
        var answer = new PinnedAnalyticsSnapshotProvider(provider);
        var old = await answer.GetLocalPathAsync(default);
        s3.Change("second");
        clock.Advance();
        var updated = await provider.GetLocalPathAsync(default);
        Assert.NotEqual(old, updated);
        Assert.Equal("second", await File.ReadAllTextAsync(updated));
        Assert.Equal(old, await answer.GetLocalPathAsync(default));
        Assert.Equal("first", await File.ReadAllTextAsync(old));
    }

    [Fact]
    public async Task InvalidReplacementDoesNotPublishOrDestroyPreviousVersion()
    {
        var provider = Provider(async (path, ct) =>
        {
            if (await File.ReadAllTextAsync(path, ct) == "invalid") throw new InvalidOperationException("Wrong region");
        });
        var old = await provider.GetLocalPathAsync(default);
        s3.Change("invalid");
        clock.Advance();
        await Assert.ThrowsAsync<InvalidOperationException>(() => provider.GetLocalPathAsync(default));
        Assert.Equal("first", await File.ReadAllTextAsync(old));
        Assert.Single(Directory.GetFiles(directory));
        s3.Change("valid");
        Assert.Equal("valid", await File.ReadAllTextAsync(await provider.GetLocalPathAsync(default)));
    }

    [Fact]
    public async Task ObjectChangedBetweenHeadAndGetFailsWithoutPublishing()
    {
        s3.RejectGet = true;
        await Assert.ThrowsAsync<InvalidOperationException>(() => Provider().GetLocalPathAsync(default));
        Assert.Empty(Directory.GetFiles(directory));
    }

    [Fact]
    public async Task TruncatedDownloadIsRemoved()
    {
        s3.ReportedLength = 100;
        await Assert.ThrowsAsync<InvalidOperationException>(() => Provider().GetLocalPathAsync(default));
        Assert.Empty(Directory.GetFiles(directory));
    }

    [Fact]
    public async Task OversizedObjectIsRejectedBeforeDownloading()
    {
        s3.ReportedLength = S3AnalyticsSnapshotProvider.MaximumSnapshotBytes + 1;
        await Assert.ThrowsAsync<InvalidOperationException>(() => Provider().GetLocalPathAsync(default));
        Assert.Equal(0, s3.Downloads);
    }

    [Fact]
    public async Task CancellationDuringValidationCleansUpAndAllowsRetry()
    {
        using var cancellation = new CancellationTokenSource();
        var provider = Provider((_, ct) => { cancellation.Cancel(); ct.ThrowIfCancellationRequested(); return Task.CompletedTask; });
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => provider.GetLocalPathAsync(cancellation.Token));
        Assert.Empty(Directory.GetFiles(directory));
        Assert.True(File.Exists(await provider.GetLocalPathAsync(default)));
    }

    [Fact]
    public async Task MissingCachedFileIsDownloadedAgain()
    {
        var provider = Provider();
        File.Delete(await provider.GetLocalPathAsync(default));
        Assert.True(File.Exists(await provider.GetLocalPathAsync(default)));
        Assert.Equal(2, s3.Downloads);
    }

    public void Dispose() { s3.Dispose(); Directory.Delete(directory, true); }
    private sealed class Clock : TimeProvider
    {
        private DateTimeOffset now = DateTimeOffset.UtcNow;
        public override DateTimeOffset GetUtcNow() => now;
        public void Advance() => now += TimeSpan.FromMinutes(6);
    }
    private sealed class FakeS3() : AmazonS3Client(new Amazon.Runtime.AnonymousAWSCredentials(), Amazon.RegionEndpoint.USWest1)
    {
        private string value = "first";
        private string etag = "\"1\"";
        public int Heads;
        public int Downloads;
        public long? ReportedLength;
        public bool RejectGet;
        public void Change(string content) { value = content; etag = "\"" + Guid.NewGuid() + "\""; }
        public override Task<GetObjectMetadataResponse> GetObjectMetadataAsync(GetObjectMetadataRequest request, CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            Heads++;
            return Task.FromResult(new GetObjectMetadataResponse { ETag = etag, ContentLength = ReportedLength ?? Encoding.UTF8.GetByteCount(value) });
        }
        public override Task<GetObjectResponse> GetObjectAsync(GetObjectRequest request, CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            Downloads++;
            Assert.Equal(etag, request.EtagToMatch);
            if (RejectGet) throw new AmazonS3Exception("Changed") { StatusCode = HttpStatusCode.PreconditionFailed };
            return Task.FromResult(new GetObjectResponse { ResponseStream = new MemoryStream(Encoding.UTF8.GetBytes(value)) });
        }
    }
}
