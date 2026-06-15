using Momento.Sdk;
using Momento.Sdk.Auth;
using Momento.Sdk.Config;
using Momento.Sdk.Responses;
using F3Core.Regions;
using System.Text.Json;
using F3Core;

namespace F3Lambda.Data
{
    public static class CacheHelper
    {
        public const string SkipMomentoEnvironmentVariable = "F3_SKIP_MOMENTO";
        private const string MomentoTokenEnvironmentVariable = "F3_MOMENTO_TOKEN";
        private static readonly Lazy<ICredentialProvider> authProvider = new(() => new EnvMomentoTokenProvider(MomentoTokenEnvironmentVariable));
        private static readonly TimeSpan DEFAULT_TTL = TimeSpan.FromHours(24);
        private static readonly string cacheName = "F3Data";

        public static bool ShouldSkipMomento =>
            IsEnabled(Environment.GetEnvironmentVariable(SkipMomentoEnvironmentVariable));

        private static bool IsEnabled(string? value)
        {
            return value != null &&
                (value.Equals("1", StringComparison.OrdinalIgnoreCase) ||
                value.Equals("true", StringComparison.OrdinalIgnoreCase) ||
                value.Equals("yes", StringComparison.OrdinalIgnoreCase));
        }

        private static string GetCacheKey(string prefix, CacheKeyType cacheKeyType)
        {
            switch (cacheKeyType)
            {
                case CacheKeyType.AllData:
                    return $"AllData_{prefix}";
                case CacheKeyType.Locations:
                    return $"Locations_{prefix}";
                case CacheKeyType.Close100s:
                    return $"Close100s_{prefix}";
                case CacheKeyType.AllDataSummary:
                    return $"AllDataSummary_{prefix}";
                case CacheKeyType.SectorData:
                    return $"SectorData_{prefix}";
                case CacheKeyType.InitialView:
                    return $"InitialView_{prefix}";
                case CacheKeyType.RegionSummary:
                    return $"RegionSummary_{prefix}";
                default:
                    return string.Empty;
            }
        }

        public static async Task<T> GetCachedDataAsync<T>(string prefix, CacheKeyType cacheKeyType)
        {
            if (ShouldSkipMomento)
            {
                Console.WriteLine("Momento cache skipped by environment setting.");
                return default(T);
            }

            var cacheKey = GetCacheKey(prefix, cacheKeyType);

            if (string.IsNullOrEmpty(cacheKey))
            {
                throw new Exception("Invalid cache key type");
            }

            using (SimpleCacheClient client = new SimpleCacheClient(Configurations.Laptop.Latest(), authProvider.Value, DEFAULT_TTL))
            {
                CacheGetResponse getResponse = await client.GetAsync(cacheName, cacheKey);

                if (getResponse is CacheGetResponse.Hit hitResponse)
                {
                    Console.WriteLine("Cache Hit");

                    // If T is a string, return the value as is
                    if (typeof(T) == typeof(string))
                    {
                        return (T)(object)hitResponse.ValueString;
                    }

                    return JsonSerializer.Deserialize<T>(hitResponse.ValueString);
                }

                if (getResponse is CacheGetResponse.Error errorResponse)
                {
                    Console.WriteLine($"Error getting cache value: {errorResponse.Message}.");
                }

                return default(T);
            }
        }

        public static async Task SetCachedDataAsync(string prefix, CacheKeyType cacheKeyType, string data)
        {
            if (ShouldSkipMomento)
            {
                Console.WriteLine("Momento cache write skipped by environment setting.");
                return;
            }

            var cacheKey = GetCacheKey(prefix, cacheKeyType);

            if (string.IsNullOrEmpty(cacheKey))
            {
                throw new Exception("Invalid cache key type");
            }

            using (SimpleCacheClient client = new SimpleCacheClient(Configurations.Laptop.Latest(), authProvider.Value, DEFAULT_TTL))
            {
                var setResponse = await client.SetAsync(cacheName, cacheKey, data);
                if (setResponse is CacheSetResponse.Error setError)
                {
                    Console.WriteLine($"Error setting cache value: {setError.Message}.");
                }
            }
        }

        public static async Task ClearAllCachedDataAsync(Region region)
        {
            if (ShouldSkipMomento)
            {
                Console.WriteLine("Momento cache clear skipped by environment setting.");
                return;
            }

            using (SimpleCacheClient client = new SimpleCacheClient(Configurations.Laptop.Latest(), authProvider.Value, DEFAULT_TTL))
            {
                // Delete each of the CacheKeyTypes
                foreach (CacheKeyType cacheKeyType in Enum.GetValues(typeof(CacheKeyType)))
                {
                    await client.DeleteAsync(cacheName, GetCacheKey(region.DisplayName, cacheKeyType));
                }
            }
        }
    }
}
