using F3Core;
using System.Text.Json;
using Xunit;

namespace F3Lambda.Tests;

public class SectorPeriodTests
{
    [Fact]
    public void ExistingRequestsDefaultToAllTime()
    {
        Assert.False(JsonSerializer.Deserialize<FunctionInput>("{\"Action\":\"GetSectorDataSummaryAsync\"}")!.ThisYear);
    }

    [Fact]
    public void ThisYearFiltersBeforeCountingAndExcludesUndatedHistory()
    {
        var summary = RegionSummaryCalculator.Calculate(Data(), 2026, new DateTime(2026, 6, 1));
        var pax = Assert.Single(summary.PaxData).Value;
        Assert.Equal("Banjo", pax.PaxName);
        Assert.Equal(2, pax.PostCount);
        Assert.Equal(1, pax.QCount);
        Assert.Equal(new DateTime(2026, 1, 1), pax.FirstPost);
    }

    [Fact]
    public void AllTimePreservesHistoricalTotalsAndHistoricalOnlyPax()
    {
        var summary = RegionSummaryCalculator.Calculate(Data(), null, new DateTime(2026, 6, 1));
        Assert.Equal(3, summary.PaxData.Count);
        Assert.Equal(14, summary.PaxData["Banjo"].PostCount);
        Assert.Equal(5, summary.PaxData["Banjo"].QCount);
        Assert.Equal(new DateTime(2020, 1, 1), summary.PaxData["Banjo"].FirstPost);
        Assert.Equal(7, summary.PaxData["Legacy"].PostCount);
    }

    [Fact]
    public void EmptyYearHasNoPaxButKeepsCurrentOverviewMetrics()
    {
        var data = Data();
        var now = new DateTime(2026, 1, 5);
        var all = RegionSummaryCalculator.Calculate(data, null, now);
        var empty = RegionSummaryCalculator.Calculate(data, 2028, now);
        Assert.Empty(empty.PaxData);
        Assert.Equal(all.AoCount, empty.AoCount);
        Assert.Equal(all.RecentUniquePaxCount, empty.RecentUniquePaxCount);
    }

    [Theory]
    [InlineData("2027-01-01T07:59:59Z", 2026)]
    [InlineData("2027-01-01T08:00:00Z", 2027)]
    public void YearRollsOverAtSacramentoMidnight(string utc, int year)
    {
        Assert.Equal(year, RegionSummaryCalculator.SacramentoYear(DateTimeOffset.Parse(utc)));
    }

    [Fact]
    public void PeriodCachesCannotMixAndNewYearGetsFreshKey()
    {
        Assert.Equal("SacSector", RegionSummaryCalculator.CachePrefix("SacSector", null));
        var keys = new[] { (int?)null, 2026, 2027 }
            .Select(year => RegionSummaryCalculator.CachePrefix("SacSector", year));
        Assert.Equal(3, keys.Distinct().Count());
        Assert.NotEqual(RegionSummaryCalculator.CachePrefix("South Fork", 2026),
            RegionSummaryCalculator.CachePrefix("Rubicon", 2026));
    }

    private static AllData Data() => new()
    {
        Posts = new()
        {
            new() { Pax = "Banjo", Date = new DateTime(2025, 12, 31), IsQ = true },
            new() { Pax = " Banjo ", Date = new DateTime(2026, 1, 1), IsQ = true },
            new() { Pax = "Banjo", Date = new DateTime(2026, 12, 31) },
            new() { Pax = "Banjo", Date = new DateTime(2027, 1, 1) },
            new() { Pax = "Prior", Date = new DateTime(2025, 12, 30) }
        },
        HistoricalData = new()
        {
            new() { PaxName = "Banjo", PostCount = 10, QCount = 3, FirstPost = new DateTime(2020, 1, 1) },
            new() { PaxName = "Legacy", PostCount = 7, FirstPost = new DateTime(2020, 2, 1) }
        },
        Aos = new() { new() { Name = "Test AO" } }
    };
}
