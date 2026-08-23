using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Amazon.S3;
using Amazon.S3.Model;
using F3Core.Regions;

namespace F3Lambda;

public sealed class RegionCatalogSnapshot
{
    public required RegionConfigCatalog Catalog { get; init; }
    public required string ETag { get; init; }
    public string VersionId { get; init; } = string.Empty;
    public DateTime? LastModifiedUtc { get; init; }
    public required string Source { get; init; }
    public bool CanWrite { get; init; }
}

public sealed class SaveRegionCatalogResult
{
    public required string ETag { get; init; }
    public string VersionId { get; init; } = string.Empty;
}

public sealed class RegionConfigConflictException : Exception
{
    public RegionConfigConflictException() : base("The region configuration changed after it was loaded.") { }
}

public interface IRegionConfigStore
{
    Task<RegionCatalogSnapshot> GetCatalogAsync(CancellationToken cancellationToken = default);
    Task<SaveRegionCatalogResult> SaveCatalogAsync(
        RegionConfigCatalog catalog,
        string expectedETag,
        CancellationToken cancellationToken = default);
}

public static class RegionConfigStoreFactory
{
    public static IRegionConfigStore? Create(IAmazonS3? s3Client = null)
    {
        var file = Environment.GetEnvironmentVariable(S3RegionConfigProvider.FileEnvironmentVariable);
        if (!string.IsNullOrWhiteSpace(file))
        {
            return new FileRegionConfigStore(file);
        }

        var disableS3 = string.Equals(
            Environment.GetEnvironmentVariable(S3RegionConfigProvider.DisableS3EnvironmentVariable),
            "true",
            StringComparison.OrdinalIgnoreCase);
        var bucket = Environment.GetEnvironmentVariable(S3RegionConfigProvider.BucketEnvironmentVariable);
        if (disableS3 || string.IsNullOrWhiteSpace(bucket))
        {
            return null;
        }

        return new S3RegionConfigStore(
            s3Client ?? new AmazonS3Client(),
            bucket,
            Environment.GetEnvironmentVariable(S3RegionConfigProvider.KeyEnvironmentVariable) ?? "regions.json");
    }
}

public sealed class S3RegionConfigStore : IRegionConfigStore
{
    private readonly IAmazonS3 s3Client;
    private readonly string bucket;
    private readonly string key;

    public S3RegionConfigStore(IAmazonS3 s3Client, string bucket, string key)
    {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.key = key;
    }

    public async Task<RegionCatalogSnapshot> GetCatalogAsync(CancellationToken cancellationToken = default)
    {
        using var response = await s3Client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucket,
            Key = key
        }, cancellationToken);
        using var reader = new StreamReader(response.ResponseStream);
        var json = await reader.ReadToEndAsync(cancellationToken);
        var catalog = RegionConfigJson.Deserialize(json);

        return new RegionCatalogSnapshot
        {
            Catalog = catalog,
            ETag = NormalizeETag(response.ETag),
            VersionId = response.VersionId ?? string.Empty,
            LastModifiedUtc = response.LastModified,
            Source = $"s3:{bucket}/{key}",
            CanWrite = true
        };
    }

    public async Task<SaveRegionCatalogResult> SaveCatalogAsync(
        RegionConfigCatalog catalog,
        string expectedETag,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var response = await s3Client.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucket,
                Key = key,
                ContentBody = RegionConfigJson.Serialize(catalog),
                ContentType = "application/json",
                IfMatch = QuoteETag(expectedETag)
            }, cancellationToken);
            return new SaveRegionCatalogResult
            {
                ETag = NormalizeETag(response.ETag),
                VersionId = response.VersionId ?? string.Empty
            };
        }
        catch (AmazonS3Exception ex) when (
            ex.StatusCode == HttpStatusCode.PreconditionFailed ||
            ex.StatusCode == HttpStatusCode.Conflict)
        {
            throw new RegionConfigConflictException();
        }
    }

    private static string NormalizeETag(string? etag) => (etag ?? string.Empty).Trim('"');
    private static string QuoteETag(string etag) => $"\"{NormalizeETag(etag)}\"";
}

public sealed class FileRegionConfigStore : IRegionConfigStore
{
    public const string AllowWritesEnvironmentVariable = "REGION_CONFIG_ALLOW_LOCAL_WRITES";
    private readonly string path;

    public FileRegionConfigStore(string path)
    {
        this.path = Path.GetFullPath(path);
    }

    public async Task<RegionCatalogSnapshot> GetCatalogAsync(CancellationToken cancellationToken = default)
    {
        var bytes = await File.ReadAllBytesAsync(path, cancellationToken);
        return new RegionCatalogSnapshot
        {
            Catalog = RegionConfigJson.Deserialize(Encoding.UTF8.GetString(bytes)),
            ETag = Hash(bytes),
            LastModifiedUtc = File.GetLastWriteTimeUtc(path),
            Source = $"file:{path}",
            CanWrite = string.Equals(
                Environment.GetEnvironmentVariable(AllowWritesEnvironmentVariable),
                "true",
                StringComparison.OrdinalIgnoreCase)
        };
    }

    public async Task<SaveRegionCatalogResult> SaveCatalogAsync(
        RegionConfigCatalog catalog,
        string expectedETag,
        CancellationToken cancellationToken = default)
    {
        if (!string.Equals(
            Environment.GetEnvironmentVariable(AllowWritesEnvironmentVariable),
            "true",
            StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Local region configuration writes are disabled. Set {AllowWritesEnvironmentVariable}=true and use an explicit development catalog path.");
        }

        var currentBytes = await File.ReadAllBytesAsync(path, cancellationToken);
        if (!CryptographicOperations.FixedTimeEquals(
            Encoding.UTF8.GetBytes(Hash(currentBytes)),
            Encoding.UTF8.GetBytes(expectedETag)))
        {
            throw new RegionConfigConflictException();
        }

        var bytes = Encoding.UTF8.GetBytes(RegionConfigJson.Serialize(catalog));
        var tempPath = Path.Combine(Path.GetDirectoryName(path)!, $".{Path.GetFileName(path)}.{Guid.NewGuid():N}.tmp");
        await File.WriteAllBytesAsync(tempPath, bytes, cancellationToken);
        File.Move(tempPath, path, true);
        return new SaveRegionCatalogResult { ETag = Hash(bytes) };
    }

    private static string Hash(byte[] value) => Convert.ToHexString(SHA256.HashData(value)).ToLowerInvariant();
}

public static class RegionConfigJson
{
    public static JsonSerializerOptions Options { get; } = CreateOptions();

    public static RegionConfigCatalog Deserialize(string json)
    {
        var catalog = JsonSerializer.Deserialize<RegionConfigCatalog>(json, Options)
            ?? throw new InvalidOperationException("Region config JSON deserialized to null.");
        catalog.MigrateLegacyOptionalColumns();
        return catalog;
    }

    public static string Serialize(RegionConfigCatalog catalog) =>
        JsonSerializer.Serialize(catalog, Options);

    private static JsonSerializerOptions CreateOptions()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = true
        };
        options.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.CamelCase));
        return options;
    }
}
