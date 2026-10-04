using F3Core;

namespace F3Lambda;

public static class RegionSummaryCalculator
{
    public static int CurrentSacramentoYear => SacramentoYear(DateTimeOffset.UtcNow);

    public static int SacramentoYear(DateTimeOffset now) =>
        TimeZoneInfo.ConvertTime(now, TimeZoneInfo.FindSystemTimeZoneById("America/Los_Angeles")).Year;

    public static string CachePrefix(string name, int? year) =>
        year.HasValue ? $"{name}_Year_{year.Value}" : name;

    public static RegionSummaryData Calculate(AllData allData, int? year, DateTime now)
    {
        // Group by pax name to get aggregated counts
        var posts = year.HasValue
            ? allData.Posts.Where(p => p.Date.Year == year.Value)
            : allData.Posts;
        var paxData = posts
            .GroupBy(x => x.Pax.Trim())
            .Select(x => new
            {
                PaxName = x.Key,
                Data = new PaxRegionData(
                    x.Key,
                    x.Count(),
                    x.Count(y => y.IsQ),
                    x.Min(p => p.Date)
                )
            })
            .ToDictionary(x => x.PaxName, x => x.Data);

        // Handle historical data if it exists
        if (!year.HasValue && allData.HistoricalData != null && allData.HistoricalData.Any())
        {
            var historicalPaxData = allData.HistoricalData
                .GroupBy(x => x.PaxName)
                .Select(x => new
                {
                    PaxName = x.Key,
                    Data = new PaxRegionData(
                        x.Key,
                        x.Sum(y => y.PostCount),
                        x.Sum(y => y.QCount),
                        x.Min(y => y.FirstPost.GetValueOrDefault())
                    )
                })
                .ToDictionary(x => x.PaxName, x => x.Data);

            // Combine current and historical data
            foreach (var histPax in historicalPaxData)
            {
                if (paxData.TryGetValue(histPax.Key, out var currentData))
                {
                    // Combine: sum counts, take earlier first post date
                    paxData[histPax.Key] = new PaxRegionData(
                        histPax.Key,
                        currentData.PostCount + histPax.Value.PostCount,
                        currentData.QCount + histPax.Value.QCount,
                        currentData.FirstPost < histPax.Value.FirstPost ? currentData.FirstPost : histPax.Value.FirstPost
                    );
                }
                else
                {
                    // Add historical data for PAX not in current data
                    paxData[histPax.Key] = histPax.Value;
                }
            }
        }

        // Calculate recent unique PAX count (last 30 days)
        var recentUniquePaxCount = allData.Posts
            .Where(x => x.Date >= now.AddDays(-30))
            .Select(x => x.Pax.Trim())
            .Distinct()
            .Count();

        // Create the RegionSummaryData object
        var regionSummary = new RegionSummaryData
        {
            PaxData = paxData,
            AoCount = allData.Aos?.Count ?? 0,
            RecentUniquePaxCount = recentUniquePaxCount
        };

        return regionSummary;
    }
}
