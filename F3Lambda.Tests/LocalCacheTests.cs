using System.Reflection;
using F3Core;
using F3Core.Regions;
using F3Lambda.Data;
using Xunit;

namespace F3Lambda.Tests;

[CollectionDefinition("Local cache environment", DisableParallelization = true)]
public class LocalCacheEnvironment { }

[Collection("Local cache environment")]
public class LocalCacheTests
{
    [Fact]
    public async Task SummaryReadsReuseCompressedSourceAndRegionWritesInvalidateLocalCache()
    {
        var skip = Environment.GetEnvironmentVariable("F3_SKIP_MOMENTO");
        var local = Environment.GetEnvironmentVariable("F3_LOCAL_MEMORY_CACHE");
        Environment.SetEnvironmentVariable("F3_SKIP_MOMENTO", "true");
        Environment.SetEnvironmentVariable("F3_LOCAL_MEMORY_CACHE", "true");
        var region = new SouthFork();
        try
        {
            const string json = "{\"posts\":[{\"pax\":\"Test PAX\"}]}";
            var compress = typeof(Function).GetMethod("Compress", BindingFlags.NonPublic | BindingFlags.Static)!;
            var packed = (string)compress.Invoke(null, new object[] { json })!;
            await CacheHelper.SetCachedDataAsync(region.DisplayName, CacheKeyType.AllData, packed);
            var read = typeof(Function).GetMethod("GetAllDataAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
            // A null Sheets client ensures a cache hit never reaches Google.
            var result = (Task<string>)read.Invoke(new Function(), new object?[] { null, region, false })!;
            Assert.Equal(json, await result);
            await CacheHelper.ClearAllCachedDataAsync(region);
            Assert.Null(await CacheHelper.GetCachedDataAsync<string>(region.DisplayName, CacheKeyType.AllData));
        }
        finally
        {
            await CacheHelper.ClearAllCachedDataAsync(region);
            Environment.SetEnvironmentVariable("F3_SKIP_MOMENTO", skip);
            Environment.SetEnvironmentVariable("F3_LOCAL_MEMORY_CACHE", local);
        }
    }
}
