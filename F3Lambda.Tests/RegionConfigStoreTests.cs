using System.Text.Json;
using System.Net;
using System.Text;
using F3Core;
using F3Core.Regions;
using Xunit;

namespace F3Lambda.Tests;

public class RegionConfigStoreTests
{
    [Fact]
    public void LegacyOptionalZeroColumnsMigrateToNull()
    {
        var catalog = RegionConfigJson.Deserialize("""
        {
          "configVersion": "legacy",
          "regions": [{
            "queryStringValue": "test",
            "displayName": "Test",
            "masterDataColumnIndicies": { "qSourcePost": 0, "qSourceQ": 2, "extraActivity": 0 },
            "aoColumnIndicies": { "retired": 0, "hasQSource": 3, "isQSourceOnly": 0 }
          }]
        }
        """);

        Assert.Equal(RegionConfigCatalog.CurrentSchemaVersion, catalog.SchemaVersion);
        Assert.Null(catalog.Regions[0].MasterDataColumnIndicies.QSourcePost);
        Assert.Equal<short?>(2, catalog.Regions[0].MasterDataColumnIndicies.QSourceQ);
        Assert.Null(catalog.Regions[0].MasterDataColumnIndicies.ExtraActivity);
        Assert.Null(catalog.Regions[0].AoColumnIndicies.Retired);
        Assert.Equal<short?>(3, catalog.Regions[0].AoColumnIndicies.HasQSource);
        Assert.Null(catalog.Regions[0].AoColumnIndicies.IsQSourceOnly);
    }

    [Fact]
    public async Task FileStoreUsesOptimisticConcurrencyAndPreservesCatalogValues()
    {
        var directory = Directory.CreateTempSubdirectory("region-config-tests-");
        var path = Path.Combine(directory.FullName, "regions.dev.json");
        var originalAllowWrites = Environment.GetEnvironmentVariable(FileRegionConfigStore.AllowWritesEnvironmentVariable);
        try
        {
            var catalog = RegionConfigCatalog.FromHardCodedRegions();
            await File.WriteAllTextAsync(path, RegionConfigJson.Serialize(catalog));
            var store = new FileRegionConfigStore(path);
            var snapshot = await store.GetCatalogAsync();
            Environment.SetEnvironmentVariable(FileRegionConfigStore.AllowWritesEnvironmentVariable, "true");

            var unchangedGlobalValues = snapshot.Catalog.DownrangeNamingRegions.Select(x => (x.Index, x.DisplayName)).ToList();
            snapshot.Catalog.Regions[0].DisplayName += " Edited";
            snapshot.Catalog.ConfigVersion = "new-version";
            var saved = await store.SaveCatalogAsync(snapshot.Catalog, snapshot.ETag);
            var reloaded = await store.GetCatalogAsync();

            Assert.NotEqual(snapshot.ETag, saved.ETag);
            Assert.Equal("new-version", reloaded.Catalog.ConfigVersion);
            Assert.Equal(unchangedGlobalValues, reloaded.Catalog.DownrangeNamingRegions.Select(x => (x.Index, x.DisplayName)).ToList());
            await Assert.ThrowsAsync<RegionConfigConflictException>(() =>
                store.SaveCatalogAsync(reloaded.Catalog, snapshot.ETag));
        }
        finally
        {
            Environment.SetEnvironmentVariable(FileRegionConfigStore.AllowWritesEnvironmentVariable, originalAllowWrites);
            directory.Delete(true);
        }
    }

    [Fact]
    public async Task FileStoreRefusesWritesUnlessExplicitlyEnabled()
    {
        var directory = Directory.CreateTempSubdirectory("region-config-read-only-tests-");
        var path = Path.Combine(directory.FullName, "regions.dev.json");
        var originalAllowWrites = Environment.GetEnvironmentVariable(FileRegionConfigStore.AllowWritesEnvironmentVariable);
        try
        {
            Environment.SetEnvironmentVariable(FileRegionConfigStore.AllowWritesEnvironmentVariable, null);
            await File.WriteAllTextAsync(path, RegionConfigJson.Serialize(RegionConfigCatalog.FromHardCodedRegions()));
            var store = new FileRegionConfigStore(path);
            var snapshot = await store.GetCatalogAsync();
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                store.SaveCatalogAsync(snapshot.Catalog, snapshot.ETag));
        }
        finally
        {
            Environment.SetEnvironmentVariable(FileRegionConfigStore.AllowWritesEnvironmentVariable, originalAllowWrites);
            directory.Delete(true);
        }
    }

    [Fact]
    public async Task GooglePermissionCheckRejectsViewerCapability()
    {
        var responses = new Queue<HttpResponseMessage>(new[]
        {
            JsonResponse("{\"sub\":\"subject-1\",\"email\":\"viewer@example.com\"}"),
            JsonResponse("{\"id\":\"sheet-1\",\"name\":\"Test\",\"mimeType\":\"application/vnd.google-apps.spreadsheet\",\"capabilities\":{\"canEdit\":false,\"canModifyContent\":false}}")
        });
        var service = new GoogleDrivePermissionService(new HttpClient(new QueueHandler(responses)));

        await Assert.ThrowsAsync<UnauthorizedAccessException>(() =>
            service.VerifyEditorAsync("short-lived-token", "sheet-1"));
    }

    [Fact]
    public async Task GooglePermissionCheckReturnsVerifiedEditorIdentity()
    {
        var responses = new Queue<HttpResponseMessage>(new[]
        {
            JsonResponse("{\"sub\":\"subject-1\",\"email\":\"editor@example.com\"}"),
            JsonResponse("{\"id\":\"sheet-1\",\"name\":\"Test\",\"mimeType\":\"application/vnd.google-apps.spreadsheet\",\"capabilities\":{\"canEdit\":true,\"canModifyContent\":true}}")
        });
        var service = new GoogleDrivePermissionService(new HttpClient(new QueueHandler(responses)));

        var actor = await service.VerifyEditorAsync("short-lived-token", "sheet-1");

        Assert.Equal("subject-1", actor.Subject);
        Assert.Equal("editor@example.com", actor.Email);
    }

    [Theory]
    [InlineData(0, "A")]
    [InlineData(25, "Z")]
    [InlineData(26, "AA")]
    [InlineData(701, "ZZ")]
    public void ColumnLettersRoundTrip(int index, string letter)
    {
        Assert.Equal(letter, OnboardingService.GetColumnLetter(index));
        Assert.Equal(index, OnboardingService.GetColumnIndex(letter));
    }

    private static HttpResponseMessage JsonResponse(string json) => new(HttpStatusCode.OK)
    {
        Content = new StringContent(json, Encoding.UTF8, "application/json")
    };

    private sealed class QueueHandler : HttpMessageHandler
    {
        private readonly Queue<HttpResponseMessage> responses;
        public QueueHandler(Queue<HttpResponseMessage> responses) => this.responses = responses;
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            Task.FromResult(responses.Dequeue());
    }
}
