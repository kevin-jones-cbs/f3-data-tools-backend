using System.Text.RegularExpressions;
using F3Core.Regions;

namespace F3Lambda.Analytics;

public static class ChatRegion
{
    public static string Normalize(string region)
    {
        if (string.IsNullOrWhiteSpace(region) || !Regex.IsMatch(region, "^[a-zA-Z0-9-]{1,64}$"))
            throw new ArgumentException("Supply a valid region ID.");
        return region.ToLowerInvariant();
    }
    public static string DisplayName(string region) => RegionList.All
        .FirstOrDefault(r => r.QueryStringValue.Equals(region, StringComparison.OrdinalIgnoreCase))?.DisplayName
        ?? System.Globalization.CultureInfo.InvariantCulture.TextInfo.ToTitleCase(region.Replace('-', ' '));
}
