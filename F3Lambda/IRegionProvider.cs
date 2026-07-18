using F3Core.Regions;

namespace F3Lambda;

public interface IRegionProvider
{
    Task<Region?> GetRegionAsync(string slug);
    Task<IReadOnlyList<Region>> GetRegionsAsync();
    Task<IReadOnlyList<RegionNamingOption>> GetDownrangeNamingRegionsAsync();
}
