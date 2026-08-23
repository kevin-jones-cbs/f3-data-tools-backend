using Amazon.S3;
using F3Core.Regions;
using System.Runtime.CompilerServices;

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

    private readonly IRegionConfigStore? configStore;

    public S3RegionConfigProvider()
        : this(RegionConfigStoreFactory.Create())
    {
    }

    public S3RegionConfigProvider(IAmazonS3 s3Client)
        : this(RegionConfigStoreFactory.Create(s3Client))
    {
    }

    public S3RegionConfigProvider(IRegionConfigStore? configStore)
    {
        this.configStore = configStore;
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
        return GetCacheSourceKey(configStore);
    }

    private async Task<RegionConfigCatalog> LoadCatalogAsync()
    {
        if (configStore == null)
        {
            var catalog = RegionConfigCatalog.FromHardCodedRegions();
            LogCatalogLoaded(catalog, "hard-coded");
            return catalog;
        }

        try
        {
            var snapshot = await configStore.GetCatalogAsync();
            LogCatalogLoaded(snapshot.Catalog, snapshot.Source);
            return snapshot.Catalog;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Unable to load region config from S3. Falling back to hard-coded regions. {ex.Message}");
            var catalog = RegionConfigCatalog.FromHardCodedRegions();
            LogCatalogLoaded(catalog, "hard-coded-fallback");
            return catalog;
        }
    }

    private static void LogCatalogLoaded(RegionConfigCatalog catalog, string source)
    {
        Console.WriteLine(
            $"Region config loaded. source={source} version={catalog.ConfigVersion} regions={catalog.Regions.Count}");
    }

    public static void ReplaceCachedCatalog(RegionConfigCatalog catalog, string source)
    {
        lock (CacheLock)
        {
            cachedCatalog = catalog;
            cachedCatalogSource = source;
            cacheExpiresUtc = DateTime.UtcNow.Add(CacheDuration);
        }
    }

    public static string GetCacheSourceKey(IRegionConfigStore? store) => store == null
        ? "hard-coded"
        : $"{store.GetType().FullName}:{RuntimeHelpers.GetHashCode(store)}";
}
