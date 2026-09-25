using Amazon.S3;
using Amazon.S3.Model;
using System.Security.Cryptography;
using System.Text;

namespace F3Lambda.Analytics;

// One provider per region and process. Published files are immutable: existing
// requests keep their version while later requests can pick up a new snapshot.
public sealed class S3AnalyticsSnapshotProvider(
    IAmazonS3 s3, string bucket, string key, string cacheDirectory,
    Func<string, CancellationToken, Task> validate,
    TimeSpan? refreshInterval = null, TimeProvider? clock = null) : IAnalyticsSnapshotProvider
{
    private readonly SemaphoreSlim gate = new(1);
    private readonly TimeProvider time = clock ?? TimeProvider.System;
    private readonly TimeSpan interval = refreshInterval ?? TimeSpan.FromMinutes(5);
    private string? currentPath;
    private string? currentETag;
    private DateTimeOffset nextCheck;
    public const long MaximumSnapshotBytes = 64 * 1024 * 1024;
    public const long MaximumCacheBytes = 256 * 1024 * 1024;

    public async Task<string> GetLocalPathAsync(CancellationToken cancellationToken)
    {
        await gate.WaitAsync(cancellationToken);
        try
        {
            if (currentPath != null && File.Exists(currentPath) && time.GetUtcNow() < nextCheck)
                return currentPath;

            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(TimeSpan.FromSeconds(30));
            var ct = timeout.Token;
            var metadata = await s3.GetObjectMetadataAsync(new GetObjectMetadataRequest
                { BucketName = bucket, Key = key }, ct);
            if (metadata.ContentLength <= 0 || metadata.ContentLength > MaximumSnapshotBytes ||
                string.IsNullOrEmpty(metadata.ETag))
                throw new InvalidOperationException("Attendance snapshot has an invalid size or missing ETag.");
            if (metadata.ETag == currentETag && currentPath != null && File.Exists(currentPath))
            {
                nextCheck = time.GetUtcNow() + interval;
                return currentPath;
            }

            Directory.CreateDirectory(cacheDirectory);
            var identity = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(
                bucket + "\n" + key + "\n" + metadata.ETag))).ToLowerInvariant();
            var destination = Path.Combine(cacheDirectory, identity + ".duckdb");
            var temporary = Path.Combine(cacheDirectory, Guid.NewGuid() + ".download");
            try
            {
                if (!File.Exists(destination))
                {
                    // Keep old versions for in-flight readers, but bound /tmp usage.
                    if (Directory.EnumerateFiles(cacheDirectory).Sum(p => new FileInfo(p).Length) +
                        metadata.ContentLength > MaximumCacheBytes)
                        throw new InvalidOperationException("Attendance cache is full; recycle the Lambda execution environment.");
                    using var response = await s3.GetObjectAsync(new GetObjectRequest
                    {
                        BucketName = bucket, Key = key, EtagToMatch = metadata.ETag
                    }, ct);
                    await using (var output = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None,
                        81920, FileOptions.Asynchronous))
                    {
                        var buffer = new byte[81920];
                        long total = 0;
                        int read;
                        while ((read = await response.ResponseStream.ReadAsync(buffer, ct)) > 0)
                        {
                            total += read;
                            if (total > metadata.ContentLength || total > MaximumSnapshotBytes)
                                throw new InvalidOperationException("Attendance download exceeded its expected size.");
                            await output.WriteAsync(buffer.AsMemory(0, read), ct);
                        }
                        if (total != metadata.ContentLength)
                            throw new InvalidOperationException("Attendance download was incomplete.");
                    }
                    await validate(temporary, ct);
                    File.Move(temporary, destination);
                }
                else
                {
                    await validate(destination, ct);
                }
                currentPath = destination;
                currentETag = metadata.ETag;
                nextCheck = time.GetUtcNow() + interval;
                return destination;
            }
            finally { if (File.Exists(temporary)) File.Delete(temporary); }
        }
        catch (AmazonS3Exception ex)
        {
            throw new InvalidOperationException("The attendance snapshot could not be retrieved from S3.", ex);
        }
        finally { gate.Release(); }
    }
}

// Create once per answer/status request, not once per process. All SQL and the
// reported snapshot hash then refer to the same file, even during a refresh.
public sealed class PinnedAnalyticsSnapshotProvider(IAnalyticsSnapshotProvider source) : IAnalyticsSnapshotProvider
{
    private readonly SemaphoreSlim gate = new(1);
    private string? path;
    public async Task<string> GetLocalPathAsync(CancellationToken cancellationToken)
    {
        await gate.WaitAsync(cancellationToken);
        try { return path ??= await source.GetLocalPathAsync(cancellationToken); }
        finally { gate.Release(); }
    }
}
