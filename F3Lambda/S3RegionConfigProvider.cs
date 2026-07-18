using System.Text.Json;
using System.Text.Json.Serialization;
using Amazon.S3;
using Amazon.S3.Model;
using F3Core.Regions;

namespace F3Lambda;

public class S3RegionConfigProvider : IRegionProvider
{
    public const string FileEnvironmentVariable = "REGION_CONFIG_FILE";
    public const string BucketEnvironmentVariable = "REGION_CONFIG_BUCKET";
    public const string KeyEnvironmentVariable = "REGION_CONFIG_KEY";
    public const string DisableS3EnvironmentVariable = "REGION_CONFIG_DISABLE_S3";

    private static readonly TimeSpan CacheDuration = TimeSpan.FromMinutes(2);
    private static readonly object CacheLock = new();
    private static RegionConfigCatalog? cachedCatalog;
    private static string? cachedCatalogSource;
    private static DateTime cacheExpiresUtc = DateTime.MinValue;

    private readonly string? bucketName;
    private readonly string objectKey;
    private readonly string? filePath;
    private readonly bool disableS3;
    private readonly IAmazonS3 s3Client;

    public S3RegionConfigProvider()
        : this(new AmazonS3Client())
    {
    }

    public S3RegionConfigProvider(IAmazonS3 s3Client)
    {
        this.s3Client = s3Client;
        filePath = Environment.GetEnvironmentVariable(FileEnvironmentVariable);
        bucketName = Environment.GetEnvironmentVariable(BucketEnvironmentVariable);
        objectKey = Environment.GetEnvironmentVariable(KeyEnvironmentVariable) ?? "regions.json";
        disableS3 = string.Equals(
            Environment.GetEnvironmentVariable(DisableS3EnvironmentVariable),
            "true",
            StringComparison.OrdinalIgnoreCase);
    }

    public async Task<Region?> GetRegionAsync(string slug)
    {
        if (string.IsNullOrWhiteSpace(slug))
        {
            return null;
        }

        var regions = await GetRegionsAsync();
        return regions.FirstOrDefault(region =>
            string.Equals(region.QueryStringValue, slug, StringComparison.OrdinalIgnoreCase));
    }

    public async Task<IReadOnlyList<Region>> GetRegionsAsync()
    {
        var catalog = await GetCatalogAsync();
        return catalog.Regions
            .Where(region => region.IsActive)
            .Select(region => (Region)new ConfiguredRegion(region))
            .ToList();
    }

    public async Task<IReadOnlyList<RegionNamingOption>> GetDownrangeNamingRegionsAsync()
    {
        var catalog = await GetCatalogAsync();
        return catalog.DownrangeNamingRegions;
    }

    private async Task<RegionConfigCatalog> GetCatalogAsync()
    {
        var catalogSource = GetCatalogSource();

        lock (CacheLock)
        {
            if (cachedCatalog != null &&
                cachedCatalogSource == catalogSource &&
                DateTime.UtcNow < cacheExpiresUtc)
            {
                return cachedCatalog;
            }
        }

        var catalog = await LoadCatalogAsync();

        lock (CacheLock)
        {
            cachedCatalog = catalog;
            cachedCatalogSource = catalogSource;
            cacheExpiresUtc = DateTime.UtcNow.Add(CacheDuration);
        }

        return catalog;
    }

    private string GetCatalogSource()
    {
        if (!string.IsNullOrWhiteSpace(filePath))
        {
            return $"file:{Path.GetFullPath(filePath)}";
        }

        if (!disableS3 && !string.IsNullOrWhiteSpace(bucketName))
        {
            return $"s3:{bucketName}/{objectKey}";
        }

        return "hard-coded";
    }

    private async Task<RegionConfigCatalog> LoadCatalogAsync()
    {
        if (!string.IsNullOrWhiteSpace(filePath))
        {
            try
            {
                var catalog = await LoadCatalogFromFileAsync(filePath);
                LogCatalogLoaded(catalog, $"file:{Path.GetFullPath(filePath)}");
                return catalog;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unable to load region config from local file '{filePath}'. Falling back. {ex.Message}");
            }
        }

        if (disableS3 || string.IsNullOrWhiteSpace(bucketName))
        {
            var catalog = RegionConfigCatalog.FromHardCodedRegions();
            LogCatalogLoaded(catalog, "hard-coded");
            return catalog;
        }

        try
        {
            using var response = await s3Client.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucketName,
                Key = objectKey
            });
            using var reader = new StreamReader(response.ResponseStream);
            var json = await reader.ReadToEndAsync();
            var catalog = JsonSerializer.Deserialize<RegionConfigCatalog>(json, GetJsonOptions());

            if (catalog == null)
            {
                throw new InvalidOperationException("Region config JSON deserialized to null.");
            }

            LogCatalogLoaded(catalog, $"s3:{bucketName}/{objectKey}");
            return catalog;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Unable to load region config from S3. Falling back to hard-coded regions. {ex.Message}");
            var catalog = RegionConfigCatalog.FromHardCodedRegions();
            LogCatalogLoaded(catalog, "hard-coded-fallback");
            return catalog;
        }
    }

    private static async Task<RegionConfigCatalog> LoadCatalogFromFileAsync(string path)
    {
        var json = await File.ReadAllTextAsync(path);
        var catalog = JsonSerializer.Deserialize<RegionConfigCatalog>(json, GetJsonOptions());

        if (catalog == null)
        {
            throw new InvalidOperationException("Region config JSON deserialized to null.");
        }

        return catalog;
    }

    private static void LogCatalogLoaded(RegionConfigCatalog catalog, string source)
    {
        Console.WriteLine(
            $"Region config loaded. source={source} version={catalog.ConfigVersion} regions={catalog.Regions.Count}");
    }

    private static JsonSerializerOptions GetJsonOptions()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };
        options.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.CamelCase));
        return options;
    }
}
