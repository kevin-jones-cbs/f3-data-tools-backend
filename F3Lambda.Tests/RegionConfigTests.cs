using F3Core.Regions;
using System.Text.Json;
using System.Text.Json.Serialization;
using Xunit;

namespace F3Lambda.Tests
{
    public class RegionConfigTests
    {
        [Fact]
        public void ConfiguredRegionsMatchHardCodedRegionProperties()
        {
            foreach (var hardCodedRegion in RegionList.All)
            {
                var configuredRegion = new ConfiguredRegion(RegionConfig.FromRegion(hardCodedRegion));

                Assert.Equal(hardCodedRegion.QueryStringValue, configuredRegion.QueryStringValue);
                Assert.Equal(hardCodedRegion.DisplayName, configuredRegion.DisplayName);
                Assert.Equal(hardCodedRegion.SpreadsheetId, configuredRegion.SpreadsheetId);
                Assert.Equal(hardCodedRegion.MasterDataSheetIds, configuredRegion.MasterDataSheetIds);
                Assert.Equal(hardCodedRegion.MasterDataSheetNames, configuredRegion.MasterDataSheetNames);
                Assert.Equal(hardCodedRegion.MissingDataRowOffset, configuredRegion.MissingDataRowOffset);
                Assert.Equal(hardCodedRegion.RosterSheetId, configuredRegion.RosterSheetId);
                Assert.Equal(hardCodedRegion.RosterSheetName, configuredRegion.RosterSheetName);
                Assert.Equal(hardCodedRegion.RosterNameColumn, configuredRegion.RosterNameColumn);
                Assert.Equal(hardCodedRegion.RosterSheetColumns, configuredRegion.RosterSheetColumns);
                Assert.Equal(hardCodedRegion.AosSheetName, configuredRegion.AosSheetName);
                Assert.Equal(hardCodedRegion.AosRetiredIndicator, configuredRegion.AosRetiredIndicator);
                Assert.Equal(hardCodedRegion.SupportsDownrange, configuredRegion.SupportsDownrange);
                Assert.Equal(hardCodedRegion.HasHistoricalData, configuredRegion.HasHistoricalData);
                Assert.Equal(hardCodedRegion.HasQSourcePosts, configuredRegion.HasQSourcePosts);
                Assert.Equal(hardCodedRegion.HasExtraActivity, configuredRegion.HasExtraActivity);

                Assert.Equal(hardCodedRegion.MasterDataColumnIndicies.Date, configuredRegion.MasterDataColumnIndicies.Date);
                Assert.Equal(hardCodedRegion.MasterDataColumnIndicies.Location, configuredRegion.MasterDataColumnIndicies.Location);
                Assert.Equal(hardCodedRegion.MasterDataColumnIndicies.PaxName, configuredRegion.MasterDataColumnIndicies.PaxName);
                Assert.Equal(hardCodedRegion.MasterDataColumnIndicies.Fng, configuredRegion.MasterDataColumnIndicies.Fng);
                Assert.Equal(hardCodedRegion.MasterDataColumnIndicies.Post, configuredRegion.MasterDataColumnIndicies.Post);
                Assert.Equal(hardCodedRegion.MasterDataColumnIndicies.Q, configuredRegion.MasterDataColumnIndicies.Q);
                Assert.Equal(hardCodedRegion.MasterDataColumnIndicies.QSourcePost, configuredRegion.MasterDataColumnIndicies.QSourcePost);
                Assert.Equal(hardCodedRegion.MasterDataColumnIndicies.QSourceQ, configuredRegion.MasterDataColumnIndicies.QSourceQ);
                Assert.Equal(hardCodedRegion.MasterDataColumnIndicies.ExtraActivity, configuredRegion.MasterDataColumnIndicies.ExtraActivity);

                Assert.Equal(hardCodedRegion.AoColumnIndicies.Name, configuredRegion.AoColumnIndicies.Name);
                Assert.Equal(hardCodedRegion.AoColumnIndicies.City, configuredRegion.AoColumnIndicies.City);
                Assert.Equal(hardCodedRegion.AoColumnIndicies.DayOfWeek, configuredRegion.AoColumnIndicies.DayOfWeek);
                Assert.Equal(hardCodedRegion.AoColumnIndicies.Retired, configuredRegion.AoColumnIndicies.Retired);
                Assert.Equal(hardCodedRegion.AoColumnIndicies.HasQSource, configuredRegion.AoColumnIndicies.HasQSource);
                Assert.Equal(hardCodedRegion.AoColumnIndicies.IsQSourceOnly, configuredRegion.AoColumnIndicies.IsQSourceOnly);
            }
        }

        [Fact]
        public void HardCodedCatalogPreservesDownrangeNamingRegions()
        {
            var catalog = RegionConfigCatalog.FromHardCodedRegions();

            Assert.Equal(RegionList.All.Count, catalog.Regions.Count);
            Assert.Equal(RegionList.AllRegionValues.Count, catalog.DownrangeNamingRegions.Count);
            Assert.Contains(catalog.DownrangeNamingRegions, x => x.Index == 14 && x.DisplayName == "Other");
        }

        [Fact]
        public void SampleJsonDeserializesToConfiguredRegions()
        {
            var json = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "TestData", "regions.sample.json"));
            var options = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            };
            options.Converters.Add(new JsonStringEnumConverter());

            var catalog = JsonSerializer.Deserialize<RegionConfigCatalog>(json, options);

            Assert.NotNull(catalog);
            Assert.Equal(2, catalog!.Regions.Count);
            Assert.Equal(3, catalog.DownrangeNamingRegions.Count);

            var southFork = new ConfiguredRegion(catalog.Regions.First(x => x.QueryStringValue == "southfork"));
            Assert.Equal("South Fork", southFork.DisplayName);
            Assert.Equal(new List<int> { 49879406, 729344821 }, southFork.MasterDataSheetIds);
            Assert.Equal(
                new List<RosterSheetColumn>
                {
                    RosterSheetColumn.Formula,
                    RosterSheetColumn.PaxName,
                    RosterSheetColumn.JoinDate,
                    RosterSheetColumn.Empty,
                    RosterSheetColumn.NamingRegionName,
                    RosterSheetColumn.EhBy
                },
                southFork.RosterSheetColumns);
            Assert.True(southFork.SupportsDownrange);
            Assert.True(southFork.IncludeInSector);
        }
    }
}
