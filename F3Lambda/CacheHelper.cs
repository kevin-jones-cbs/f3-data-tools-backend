using Momento.Sdk;
using Momento.Sdk.Auth;
using Momento.Sdk.Config;
using Momento.Sdk.Responses;
using F3Core.Regions;
using System.Text.Json;

namespace F3Lambda.Data
{
    public enum CacheKeyType
    {
        AllData, Locations, Close100s
    }

    public static class CacheHelper
    {
        private static ICredentialProvider authProvider = new EnvMomentoTokenProvider("F3_MOMENTO_TOKEN");
        private static TimeSpan DEFAULT_TTL = TimeSpan.FromHours(24);
        private static string cacheName = "F3Data";
        private static string GetCacheKey(Region region, CacheKeyType cacheKeyType)
        {
            switch (cacheKeyType)
            {
                case CacheKeyType.AllData:
                    return $"AllData_{region.DisplayName}";
                case CacheKeyType.Locations:
                    return $"Locations_{region.DisplayName}";
                case CacheKeyType.Close100s:
                    return $"Close100s_{region.DisplayName}";
                default:
                    return string.Empty;
            }
        }

        public static async Task<T> GetCachedDataAsync<T>(Region region, CacheKeyType cacheKeyType)
        {
            var cacheKey = GetCacheKey(region, cacheKeyType);

            if (string.IsNullOrEmpty(cacheKey))
            {
                throw new Exception("Invalid cache key type");
            }

            using (SimpleCacheClient client = new SimpleCacheClient(Configurations.Laptop.Latest(), authProvider, DEFAULT_TTL))
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

        public static async Task SetCachedDataAsync(Region region, CacheKeyType cacheKeyType, string data)
        {
            var cacheKey = GetCacheKey(region, cacheKeyType);

            if (string.IsNullOrEmpty(cacheKey))
            {
                throw new Exception("Invalid cache key type");
            }

            using (SimpleCacheClient client = new SimpleCacheClient(Configurations.Laptop.Latest(), authProvider, DEFAULT_TTL))
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
            using (SimpleCacheClient client = new SimpleCacheClient(Configurations.Laptop.Latest(), authProvider, DEFAULT_TTL))
            {
                // Delete each of the CacheKeyTypes
                foreach (CacheKeyType cacheKeyType in Enum.GetValues(typeof(CacheKeyType)))
                {
                    await client.DeleteAsync(cacheName, GetCacheKey(region, cacheKeyType));
                }
            }
        }
    }
}