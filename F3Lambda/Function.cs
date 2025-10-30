using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using F3Core;
using F3Core.Regions;
using F3Lambda.Data;
using F3Lambda.Models;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Services;
using Google.Apis.Sheets.v4;
using Google.Apis.Sheets.v4.Data;
using System.IO.Compression;
using System.Text;
using System.Text.Json;

using System;
using System.Collections.Generic;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]
namespace F3Lambda;

public class Function
{
    private static readonly List<string> PaxNameBlacklist = new List<string> { "(Archived)", "(<18)" };

    public async Task<object> FunctionHandler(APIGatewayHttpApiV2ProxyRequest request, ILambdaContext context)
    {
        try
        {
            // Deserialize the request body into a FunctionInput object
            var functionInput = System.Text.Json.JsonSerializer.Deserialize<FunctionInput>(request.Body);
            object result = null;
            Console.WriteLine("In Function Handler " + functionInput.Action + " " + request.Body);

            // For Cold Starts
            if (functionInput.Action == "Awake")
            {
                result = "Awake 2";
            }

            // Get the region
            var region = RegionList.GetRegion(functionInput.Region);

            if (region == null && functionInput.Action != "GetSectorData")
            {
                result = "Error, no region specified";
            }

            var sheetsService = GetSheetsService();

            // Get recent posts
            if (functionInput.Action == "GetMissingAos")
            {
                var recentPosts = await GetMissingAosAsync(sheetsService, region);
                result = recentPosts;
            }

            // Get The Pax
            if (functionInput.Action == "GetPax")
            {
                var paxNames = await GetPaxNamesAsync(sheetsService, region);
                result = paxNames;
            }

            // Add Pax
            if (functionInput.Action == "AddPax")
            {
                await AddPaxToSheetAsync(sheetsService, region, functionInput.Pax, functionInput.QDate, functionInput.AoName, functionInput.IsQSource);
                result = "Pax Added";
            }

            // Get all posts
            if (functionInput.Action == "GetAllPosts")
            {
                var allPosts = await GetAllDataAsync(sheetsService, region);
                result = allPosts;
            }

            // GetPaxFromComment
            if (functionInput.Action == "GetPaxFromComment")
            {
                var pax = await GetPaxFromCommentAsync(sheetsService, region, functionInput.Comment);
                result = pax;
            }

            // CheckClose100s
            if (functionInput.Action == "CheckClose100s")
            {
                await CheckClose100sAsync(sheetsService, region);
                result = "Done";
            }

            // ClearCache
            if (functionInput.Action == "ClearCache")
            {
                await CacheHelper.ClearAllCachedDataAsync(region);
                result = "Cache Cleared";
            }

            // GetLocations
            if (functionInput.Action == "GetLocations")
            {
                var locations = await GetLocationsAsync(sheetsService, region);
                result = locations;
            }

            // GetJson
            if (functionInput.Action == "GetJson")
            {
                var allData = await GetJson(sheetsService, region, functionInput.JsonRow);
                result = allData;
            }

            // SaveJson
            if (functionInput.Action == "SaveJson")
            {
                await SaveJson(sheetsService, region, functionInput.Json, functionInput.JsonRow);
                result = "Json Saved";
            }

            // Save to cache
            if (functionInput.Action == "SaveToCache")
            {
                await CacheHelper.SetCachedDataAsync(region.DisplayName, functionInput.CacheKey, functionInput.CacheValue);
                result = "Saved to Cache";
            }

            // Get from cache
            if (functionInput.Action == "GetFromCache")
            {
                var cacheValue = await CacheHelper.GetCachedDataAsync<string>(region.DisplayName, functionInput.CacheKey);
                result = cacheValue;
                if (result == null)
                {
                    result = "Miss";
                }
            }

            // GetSectorData
            if (functionInput.Action == "GetSectorData")
            {
                var sectorData = await GetSectorDataAsync(sheetsService);
                result = sectorData;
            }

            // GetTerracottaChallenge
            if (functionInput.Action == "GetTerracottaChallenge")
            {
                var terracottaChallenge = await GetTerracottaChallengeAsync(sheetsService);
                result = terracottaChallenge;
            }

            if (result == null)
            {
                result = "Error, unknown action";
            }

            var response = new APIGatewayProxyResponse
            {
                StatusCode = 200,
                Body = JsonSerializer.Serialize(result),
                Headers = new Dictionary<string, string>
                {
                    { "Content-Type", "text/plain" }
                }
            };

            return result;

        }
        catch (System.Exception ex)
        {
            Console.WriteLine(ex.Message);
            Console.WriteLine(ex.InnerException?.Message ?? string.Empty);
            return new APIGatewayProxyResponse
            {
                StatusCode = 500,
                Body = JsonSerializer.Serialize("Error" + ex.Message),
                Headers = new Dictionary<string, string> { { "Content-Type", "text/plain" } }
            };
        }
    }

    private async Task CheckClose100sAsync(SheetsService sheetsService, Region region)
    {
        var allDataCompressed = await GetAllDataAsync(sheetsService, region, compress: false);

        var options = new JsonSerializerOptions
        {
            IgnoreNullValues = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
        var allData = JsonSerializer.Deserialize<AllData>(allDataCompressed, options);

        // Group by pax post count
        var paxPostCount = allData.Posts.GroupBy(x => x.Pax).Select(x => new Close100 { Name = x.Key, PostCount = x.Count() }).ToList();

        // The abyss pax names
        var abyssPax = new List<string> { "Bandwagon" };
        paxPostCount = paxPostCount.Where(x => !abyssPax.Contains(x.Name)).ToList();

        // Find anyone with 95, 195, etc posts
        var close100s = paxPostCount.Where(x => x.PostCount % 100 >= 95).ToList();
        var notify100s = close100s;

        // If there are any, check the cache to see if we've already notified
        var last100s = await CacheHelper.GetCachedDataAsync<List<Close100>>(region.DisplayName, CacheKeyType.Close100s);

        if (last100s != null)
        {
            notify100s = close100s.Where(x => !last100s.Any(y => y.Name == x.Name)).ToList();
        }

        // If there are any, notify
        if (notify100s.Any())
        {
            var message = $"Close 100s:{Environment.NewLine}";
            foreach (var pax in notify100s)
            {
                message += $"{pax.Name} - {pax.PostCount}{Environment.NewLine}";
            }

            // Send an email
            EmailPeacock.Send("Close 100s", message);
        }

        // Wait 10 seconds to save to cache to ensure it's picked up 24 hours later.
        await Task.Delay(10000);

        // Save to cache
        var serialized = JsonSerializer.Serialize(close100s);
        await CacheHelper.SetCachedDataAsync(region.DisplayName, CacheKeyType.Close100s, serialized);
    }

    private async Task<List<Pax>> GetPaxFromCommentAsync(SheetsService sheetsService, Region region, string comment)
    {
        var allPax = await GetPaxNamesAsync(sheetsService, region);
        var pax = PaxHelper.GetPaxFromComment(comment, allPax);

        return pax;
    }

    private async Task<string> GetJson(SheetsService sheetsService, Region region, short row)
    {
        var masterDataSheet = await sheetsService.Spreadsheets.Values.Get(region.SpreadsheetId, $"JSON!A{row}").ExecuteAsync();
        var json = masterDataSheet.Values.FirstOrDefault()?.FirstOrDefault()?.ToString();
        return json;
    }

    private async Task SaveJson(SheetsService sheetsService, Region region, string json, short row)
    {
        var valueRange = new ValueRange
        {
            Values = new List<IList<object>> { new List<object> { json } }
        };

        var updateRequest = sheetsService.Spreadsheets.Values.Update(valueRange, region.SpreadsheetId, $"JSON!A{row}");
        updateRequest.ValueInputOption = SpreadsheetsResource.ValuesResource.UpdateRequest.ValueInputOptionEnum.RAW;
        await updateRequest.ExecuteAsync();
    }

    private async Task<string> GetAllDataAsync(SheetsService sheetsService, Region region, bool compress = true)
    {
        var cacheKeyType = CacheKeyType.AllData;
        var cachedData = await CacheHelper.GetCachedDataAsync<string>(region.DisplayName, cacheKeyType);

        if (cachedData != null && compress)
        {
            return cachedData;
        }

        var masterDataSheet = await sheetsService.Spreadsheets.Values.Get(region.SpreadsheetId, $"{region.MasterDataSheetName}!A2:R").ExecuteAsync();

        // If the region has historical data, get it
        List<HistoricalData> historicalData = null;
        if (region.HasHistoricalData)
        {
            historicalData = masterDataSheet.Values
            // Look for site named "Archive"
            .Where(x => x.Count > region.MasterDataColumnIndicies.Location ? x[region.MasterDataColumnIndicies.Location].ToString() == "Archive" : false)    
            // Grab the name, post count, and Q count
            .Select(x => new HistoricalData
            {
                PaxName = x.Count > region.MasterDataColumnIndicies.PaxName ? x[region.MasterDataColumnIndicies.PaxName].ToString() : string.Empty,
                PostCount = x.Count > region.MasterDataColumnIndicies.Post && int.TryParse(x[region.MasterDataColumnIndicies.Post].ToString(), out var postCount) ? postCount : 0,
                QCount = x.Count > region.MasterDataColumnIndicies.Q && int.TryParse(x[region.MasterDataColumnIndicies.Q].ToString(), out var qCount) ? qCount : 0,
                FirstPost = x.Count > region.MasterDataColumnIndicies.Date ? DateTime.Parse(x[region.MasterDataColumnIndicies.Date].ToString()) : DateTime.MinValue
            })
            // Group by pax name as there may be multiple entries for the same pax
            .GroupBy(x => x.PaxName)
            // Sum the post and Q counts
            .Select(x => new HistoricalData
            {
                PaxName = x.Key,
                PostCount = x.Sum(y => y.PostCount),
                QCount = x.Sum(y => y.QCount),
                FirstPost = x.Min(y => y.FirstPost)
            }).ToList();

            // Remove the historical data from the master data
            masterDataSheet.Values = masterDataSheet.Values
                .Where(x => !(x.Count > region.MasterDataColumnIndicies.Location ? x[region.MasterDataColumnIndicies.Location].ToString() == "Archive" : false))
                .ToList();
        }

        List<Post> qSourcePosts = null;
        if (region.HasQSourcePosts)
        {
            // Get the Q Source posts
            qSourcePosts = masterDataSheet.Values
                .Where(x => x.Count > region.MasterDataColumnIndicies.QSourcePost && x[region.MasterDataColumnIndicies.QSourcePost].ToString() == "1")
                .Select(x => new Post
                {
                    Date = DateTime.Parse(x[region.MasterDataColumnIndicies.Date].ToString()),
                    Site = x.Count > region.MasterDataColumnIndicies.Location ? x[region.MasterDataColumnIndicies.Location].ToString() : string.Empty,
                    Pax = x.Count > region.MasterDataColumnIndicies.PaxName ? x[region.MasterDataColumnIndicies.PaxName].ToString() : string.Empty,
                    IsQ = x.Count > region.MasterDataColumnIndicies.QSourceQ ? x[region.MasterDataColumnIndicies.QSourceQ].ToString() == "1" : false,
                }).ToList();

            // Remove the q source posts from the master data (unless it's attached to a normal post)
            masterDataSheet.Values = masterDataSheet.Values
                .Where(x => !(x.Count > region.MasterDataColumnIndicies.QSourcePost && x[region.MasterDataColumnIndicies.QSourcePost]?.ToString() == "1") ||
                             (x.Count > region.MasterDataColumnIndicies.Post && x[region.MasterDataColumnIndicies.Post]?.ToString() == "1"))
                .ToList();

        }

        var posts = masterDataSheet.Values.Where(x => x.Count > region.MasterDataColumnIndicies.Date && DateTime.TryParse(x[region.MasterDataColumnIndicies.Date].ToString(), out var date)).Select(x => new Post
        {
            Date = DateTime.Parse(x[region.MasterDataColumnIndicies.Date].ToString()),
            Site = x.Count > region.MasterDataColumnIndicies.Location ? x[region.MasterDataColumnIndicies.Location].ToString() : string.Empty,
            Pax = x.Count > region.MasterDataColumnIndicies.PaxName ? x[region.MasterDataColumnIndicies.PaxName].ToString() : string.Empty,
            IsQ = x.Count > region.MasterDataColumnIndicies.Q ? x[region.MasterDataColumnIndicies.Q].ToString() == "1" : false,
            IsFNG = x.Count > region.MasterDataColumnIndicies.Fng ? x[region.MasterDataColumnIndicies.Fng].ToString() == "1" : false,
            ExtraActivity = region.HasExtraActivity && x.Count > region.MasterDataColumnIndicies.ExtraActivity && x[region.MasterDataColumnIndicies.ExtraActivity].ToString() == "1"
        }).ToList();

        // Get the roster
        var rosterSheet = await sheetsService.Spreadsheets.Values.Get(region.SpreadsheetId, $"{region.RosterSheetName}!A2:F").ExecuteAsync();

        var paxNameIndex = region.RosterSheetColumns.IndexOf(RosterSheetColumn.PaxName);
        var joinDateIndex = region.RosterSheetColumns.IndexOf(RosterSheetColumn.JoinDate);
        var namingRegionNameIndex = region.RosterSheetColumns.IndexOf(RosterSheetColumn.NamingRegionName) == -1 ? region.RosterSheetColumns.IndexOf(RosterSheetColumn.NamingRegionYN) : region.RosterSheetColumns.IndexOf(RosterSheetColumn.NamingRegionName);

        var pax = rosterSheet.Values.Select(x => new Pax
        {
            Name = paxNameIndex < x.Count ? x[paxNameIndex].ToString() : string.Empty,
            DateJoined = joinDateIndex < x.Count && x[joinDateIndex].ToString().Contains("/") ? DateTime.Parse(x[joinDateIndex].ToString()).ToShortDateString() : string.Empty,
            NamingRegion = namingRegionNameIndex == -1 || namingRegionNameIndex >= x.Count ? string.Empty : x[namingRegionNameIndex]?.ToString() ?? string.Empty
        }).ToList();

        // Clear out any roster items without names
        pax = pax.Where(x => !string.IsNullOrEmpty(x.Name)).ToList();

        // Get the AOs
        var aos = await GetLocationsAsync(sheetsService, region);

        var rtn = new AllData
        {
            Posts = posts,
            Pax = pax,
            Aos = aos,
            HistoricalData = historicalData,
            QSourcePosts = qSourcePosts
        };

        // Exclude any pax that contain any of the blacklist words (PaxNameBlacklist) List<string>
        rtn.Pax = rtn.Pax.Where(x => !PaxNameBlacklist.Any(y => x.Name.Contains(y))).ToList();
        rtn.Posts = rtn.Posts.Where(x => !PaxNameBlacklist.Any(y => x.Pax.Contains(y))).ToList();

        // Json serialize the object as small in size as possible
        var options = new JsonSerializerOptions
        {
            IgnoreNullValues = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };

        var json = JsonSerializer.Serialize(rtn, options);

        // Compress the json
        var compressedJson = Compress(json);

        // Save to cache
        await CacheHelper.SetCachedDataAsync(region.DisplayName, cacheKeyType, compressedJson);

        // Deserialize the json
        return compress ? compressedJson : json;

        // Inline Functions
        static string Compress(string plainText)
        {
            var buffer = Encoding.UTF8.GetBytes(plainText);
            using var memoryStream = new MemoryStream();

            var lengthBytes = BitConverter.GetBytes((int)buffer.Length);
            memoryStream.Write(lengthBytes, 0, lengthBytes.Length);

            using var gZipStream = new GZipStream(memoryStream, CompressionMode.Compress);

            gZipStream.Write(buffer, 0, buffer.Length);
            gZipStream.Flush();

            var gZipBuffer = memoryStream.ToArray();

            return Convert.ToBase64String(gZipBuffer);
        }
    }

    private async Task<List<Ao>> GetMissingAosAsync(SheetsService sheetsService, Region region)
    {
        try
        {
            var rtn = new List<Ao>();
            var valueRange = await sheetsService.Spreadsheets.Values.Get(region.SpreadsheetId, $"{region.MasterDataSheetName}!A{region.MissingDataRowOffset}:Q").ExecuteAsync();

            var dateIndex = region.MasterDataColumnIndicies.Date;
            var aoIndex = region.MasterDataColumnIndicies.Location;
            var qSourcePostIndex = region.MasterDataColumnIndicies.QSourcePost;

            // Get all posts with date, AO name, and post type
            var allPosts = valueRange.Values.Select(x => new
            {
                Dates = DateTime.Parse(x[dateIndex].ToString()),
                AoName = x[aoIndex].ToString(),
                IsQSourcePost = region.HasQSourcePosts && x.Count > qSourcePostIndex && x[qSourcePostIndex]?.ToString() == "1"
            }).ToList();

            var aoList = await GetLocationsAsync(sheetsService, region);

            // Foreach loop for the last 7 days
            for (int i = 6; i >= 0; i--)
            {
                var date = DateTime.Now.AddDays(-i);
                var dayOfWeek = date.DayOfWeek;

                // Get the AOs for the day of the week
                var aos = aoList.Where(x => x.DayOfWeek == dayOfWeek).ToList();
                foreach (var ao in aos)
                {
                    // Check if there is a regular post for the date and AO
                    var regularPostExists = allPosts.Any(x => x.Dates.Date == date.Date && x.AoName == ao.Name && !x.IsQSourcePost);
                    if (!regularPostExists)
                    {
                        // Add the missing regular post to the list
                        var missingAo = new Ao
                        {
                            Name = ao.Name,
                            City = ao.City,
                            DayOfWeek = ao.DayOfWeek,
                            Date = date,
                            HasQSource = false // This is for a regular post
                        };

                        rtn.Add(missingAo);
                    }

                    // If the AO has Q Source, check for missing Q Source posts
                    if (ao.HasQSource)
                    {
                        var qSourcePostExists = allPosts.Any(x => x.Dates.Date == date.Date && x.AoName == ao.Name && x.IsQSourcePost);
                        if (!qSourcePostExists)
                        {
                            // Add the missing Q Source post to the list
                            var missingQSourceAo = new Ao
                            {
                                Name = ao.Name,
                                City = ao.City,
                                DayOfWeek = ao.DayOfWeek,
                                Date = date,
                                HasQSource = true // This is for a Q Source post
                            };

                            rtn.Add(missingQSourceAo);
                        }
                    }
                }
            }

            return rtn;
        }
        catch (System.Exception ex)
        {
            Console.WriteLine(ex.Message);
            throw;
        }
    }

    private async Task<List<string>> GetPaxNamesAsync(SheetsService sheetsService, Region region)
    {
        var result = await sheetsService.Spreadsheets.Values.Get(region.SpreadsheetId, $"{region.RosterSheetName}!{region.RosterNameColumn}:{region.RosterNameColumn}").ExecuteAsync();
        var paxMembers = result.Values.Select(x => x.FirstOrDefault()?.ToString()).Where(x => x != null).Distinct().ToList();

        // Exclude any "archived" pax
        paxMembers = paxMembers.Where(x => !x.Contains("(Archived)")).ToList();

        return paxMembers;
    }

    private async Task<List<Ao>> GetLocationsAsync(SheetsService sheetsService, Region region)
    {
        var cacheKeyType = CacheKeyType.Locations;
        var cachedData = await CacheHelper.GetCachedDataAsync<List<Ao>>(region.DisplayName, cacheKeyType);
        if (cachedData != null)
        {
            return cachedData;
        }

        var aoResult = await sheetsService.Spreadsheets.Values.Get(region.SpreadsheetId, $"{region.AosSheetName}!A2:S").ExecuteAsync();
        var aos = new List<Ao>();

        foreach (var row in aoResult.Values)
        {
            // Found an empty row, we're done, as there is likely data below this we don't want
            if (row.Count < 1) break; 

            if (Enum.TryParse(row[region.AoColumnIndicies.DayOfWeek].ToString(), out DayOfWeek _) &&
                (row.Count <= region.AoColumnIndicies.Retired ||
                row[region.AoColumnIndicies.Retired].ToString() == region.AosRetiredIndicator)) // Ensure it's not retired
            {
                aos.Add(new Ao
                {
                    Name = row[region.AoColumnIndicies.Name].ToString(),
                    City = row[region.AoColumnIndicies.City].ToString(),
                    DayOfWeek = (DayOfWeek)Enum.Parse(typeof(DayOfWeek), row[region.AoColumnIndicies.DayOfWeek].ToString()),
                    HasQSource = region.AoColumnIndicies.HasQSource > 0 && 
                                 row.Count > region.AoColumnIndicies.HasQSource && 
                                 row[region.AoColumnIndicies.HasQSource]?.ToString()?.ToUpper() == "TRUE"
                });
            }
        }

        // Save to Cache
        var serialized = JsonSerializer.Serialize(aos);
        await CacheHelper.SetCachedDataAsync(region.DisplayName, cacheKeyType, serialized);

        return aos;
    }

    private async Task AddPaxToSheetAsync(SheetsService sheetsService, Region region, List<Pax> pax, DateTime qDate, string ao, bool isQSource)
    {
         var batchUpdateRequest = new BatchUpdateSpreadsheetRequest
        {
            Requests = new List<Request>()
        };

        // Get the sheet to find out the row count
        var masterDataCount = await GetSheetRowCountAsync(sheetsService, region.SpreadsheetId, $"{region.MasterDataSheetName}!A:A");

        // Date
        var dateUpdate = GetDefaultUpdateCellsRequest();
        dateUpdate.Start.ColumnIndex = region.MasterDataColumnIndicies.Date;
        foreach (var member in pax)
        {
            dateUpdate.Rows.Add(new RowData
            {
                Values = new List<CellData> { new CellData { UserEnteredValue = new ExtendedValue { NumberValue = qDate.Date.ToOADate() } } }
            });
        }

        batchUpdateRequest.Requests.Add(new Request { UpdateCells = dateUpdate });

        // Location
        var locationUpdate = GetDefaultUpdateCellsRequest();
        locationUpdate.Start.ColumnIndex = region.MasterDataColumnIndicies.Location;
        foreach (var member in pax)
        {
            locationUpdate.Rows.Add(new RowData
            {
                Values = new List<CellData> { new CellData { UserEnteredValue = new ExtendedValue { StringValue = ao } } }
            });
        }

        batchUpdateRequest.Requests.Add(new Request { UpdateCells = locationUpdate });

        // Pax Name
        var paxNameUpdate = GetDefaultUpdateCellsRequest();
        paxNameUpdate.Start.ColumnIndex = region.MasterDataColumnIndicies.PaxName;
        foreach (var member in pax)
        {
            paxNameUpdate.Rows.Add(new RowData
            {
                Values = new List<CellData> { new CellData { UserEnteredValue = new ExtendedValue { StringValue = member.Name } } }
            });
        }

        batchUpdateRequest.Requests.Add(new Request { UpdateCells = paxNameUpdate });

        // FNG Column
        var fngUpdate = GetDefaultUpdateCellsRequest();
        fngUpdate.Start.ColumnIndex = region.MasterDataColumnIndicies.Fng;
        foreach (var member in pax)
        {
            fngUpdate.Rows.Add(new RowData
            {
                Values = new List<CellData> { new CellData { UserEnteredValue = new ExtendedValue { NumberValue = member.IsFng && !member.IsDr ? 1 : (double?)null } } }
            });
        }

        batchUpdateRequest.Requests.Add(new Request { UpdateCells = fngUpdate });

        // Post Column
        var postUpdate = GetDefaultUpdateCellsRequest();
        postUpdate.Start.ColumnIndex = isQSource ? region.MasterDataColumnIndicies.QSourcePost : region.MasterDataColumnIndicies.Post;
        foreach (var member in pax)
        {
            postUpdate.Rows.Add(new RowData
            {
                Values = new List<CellData> { new CellData { UserEnteredValue = new ExtendedValue { NumberValue = 1 } } }
            });
        }

        batchUpdateRequest.Requests.Add(new Request { UpdateCells = postUpdate });

        // Q Column
        var qUpdate = GetDefaultUpdateCellsRequest();
        qUpdate.Start.ColumnIndex = isQSource ? region.MasterDataColumnIndicies.QSourceQ : region.MasterDataColumnIndicies.Q;
        foreach (var member in pax)
        {
            qUpdate.Rows.Add(new RowData
            {
                Values = new List<CellData> { new CellData { UserEnteredValue = new ExtendedValue { NumberValue = member.IsQ ? 1 : (double?)null } } }
            });
        }

        batchUpdateRequest.Requests.Add(new Request { UpdateCells = qUpdate });

        if (region.HasExtraActivity)
        {
            var extraActivityUpdate = GetDefaultUpdateCellsRequest();
            extraActivityUpdate.Start.ColumnIndex = region.MasterDataColumnIndicies.ExtraActivity;
            foreach (var member in pax)
            {
                extraActivityUpdate.Rows.Add(new RowData
                {
                    Values = new List<CellData> { new CellData { UserEnteredValue = new ExtendedValue { NumberValue = member.ExtraActivity ? 1 : (double?)null } } }
                });
            }

            batchUpdateRequest.Requests.Add(new Request { UpdateCells = extraActivityUpdate });
        }

        // If there are any fngs, do another UpdateCellsRequest for the roster sheet
        if (pax.Any(x => x.IsFng))
        {
            // Get the number of roster rows (look at thhe name column)
            var regionNameColumn = region.RosterSheetColumns.IndexOf(RosterSheetColumn.PaxName);
            var regionNameLetter = (char)(regionNameColumn + 65);
            var rosterCount = await GetSheetRowCountAsync(sheetsService, region.SpreadsheetId, $"{region.RosterSheetName}!{regionNameLetter}:{regionNameLetter}");

            var updateFngCellsRequest = new UpdateCellsRequest
            {
                Start = new GridCoordinate
                {
                    SheetId = region.RosterSheetId,
                    RowIndex = rosterCount,
                    ColumnIndex = 0
                },
                Rows = new List<RowData>(),
                Fields = "userEnteredValue"
            };

            foreach (var member in pax.Where(x => x.IsFng))
            {
                var namingRegion = region.DisplayName;
                if (member.IsDr)
                {
                    namingRegion = RegionList.AllRegionValues.First(x => x.Key == member.NamingRegionIndex).Value;
                }

                var rowData = new RowData();
                rowData.Values = new List<CellData>();

                foreach (var rosterColumn in region.RosterSheetColumns)
                {
                    switch (rosterColumn)
                    {
                        case RosterSheetColumn.Formula:
                            rowData.Values.Add(new CellData { UserEnteredValue = null });
                            break;
                        case RosterSheetColumn.PaxName:
                            rowData.Values.Add(new CellData { UserEnteredValue = new ExtendedValue { StringValue = member.Name } });
                            break;
                        case RosterSheetColumn.JoinDate:
                            rowData.Values.Add(new CellData { UserEnteredValue = new ExtendedValue { NumberValue = qDate.Date.ToOADate() } });
                            break;
                        case RosterSheetColumn.Empty:
                            rowData.Values.Add(new CellData { UserEnteredValue = new ExtendedValue { StringValue = string.Empty } });
                            break;
                        case RosterSheetColumn.NamingRegionName:
                            rowData.Values.Add(new CellData { UserEnteredValue = new ExtendedValue { StringValue = namingRegion } });
                            break;
                        case RosterSheetColumn.NamingRegionYN:
                            rowData.Values.Add(new CellData { UserEnteredValue = new ExtendedValue { StringValue = member.IsDr ? "Y" : "N" } });
                            break;
                    }
                }

                updateFngCellsRequest.Rows.Add(rowData);
            }

            batchUpdateRequest.Requests.Add(new Request
            {
                UpdateCells = updateFngCellsRequest
            });
        }

        var batchUpdateResponse = await sheetsService.Spreadsheets.BatchUpdate(batchUpdateRequest, region.SpreadsheetId).ExecuteAsync();

        try
        {
            // Purge the cache
            await CacheHelper.ClearAllCachedDataAsync(region);
        }
        catch (Exception ex)
        {
            // Log it but don't throw
            Console.WriteLine("Error purging cache " + ex.Message);
        }

        UpdateCellsRequest GetDefaultUpdateCellsRequest()
        {
            var updateCellsRequest = new UpdateCellsRequest
            {
                Start = new GridCoordinate
                {
                    SheetId = region.MasterDataSheetId,
                    RowIndex = masterDataCount
                },
                Rows = new List<RowData>(),
                Fields = "userEnteredValue"
            };

            return updateCellsRequest;
        }
    }

    private async Task<SectorData> GetSectorDataAsync(SheetsService sheetsService)
    {
        // Check cache
        var cachedData = await CacheHelper.GetCachedDataAsync<SectorData>("SacSector", CacheKeyType.SectorData);
        if (cachedData != null)
        {
            return cachedData;
        }

        var allRegions = RegionList.All.Where(x => !x.DisplayName.Contains("fia", StringComparison.OrdinalIgnoreCase)).ToList();

        // Get the data for each region in parallel
        var tasks = allRegions.Select(async region =>
        {
            var allDataJson = await GetAllDataAsync(sheetsService, region, false);
            return (region, allDataJson);
        });

        var allRegionData = await Task.WhenAll(tasks);

        // Deserialize and store in a dictionary
        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };

        var allRegionDataDict = allRegionData.ToDictionary(
            result => result.region,
            result => JsonSerializer.Deserialize<AllData>(result.allDataJson, options)
        );

        // Create the sector data
        var paxSectorData = new List<PaxSectorData>();

        foreach (var region in allRegionDataDict)
        {
            // Group by pax name
            var paxPostCount = region.Value.Posts.GroupBy(x => x.Pax.Trim()).Select(x => new PaxRegionData(x.Key, x.Count(), x.Count(y => y.IsQ), x.Min(x => x.Date))).ToList();

            // Handle historical data
            if (region.Value.HistoricalData != null && region.Value.HistoricalData.Any())
            {
                // Group by pax name
                var historicalPaxPostCount = region.Value.HistoricalData.GroupBy(x => x.PaxName).Select(x => new PaxRegionData(x.Key, x.Sum(y => y.PostCount), x.Sum(y => y.QCount), x.Min(y => y.FirstPost.GetValueOrDefault()))).ToList();

                // Create a dictionary for quick lookups
                var historicalDict = historicalPaxPostCount.ToDictionary(h => h.PaxName, h => h);

                // Combine both lists, ensuring no data is lost
                paxPostCount = paxPostCount
                    .Select(x =>
                        historicalDict.TryGetValue(x.PaxName, out var y)
                            ? new PaxRegionData(x.PaxName, x.PostCount + y.PostCount, x.QCount + y.QCount, x.FirstPost < y.FirstPost ? x.FirstPost : y.FirstPost)
                            : x) // If no match, keep x as is
                    .Concat(historicalPaxPostCount.Where(y => !paxPostCount.Any(x => x.PaxName == y.PaxName))) // Add unmatched historical data
                    .ToList();
            }

            // Add the region data to the sector data for each pax
            foreach (var pax in paxPostCount)
            {
                // Find the pax in the sector data
                var sectorPax = paxSectorData.FirstOrDefault(x => x.PaxName == pax.PaxName);

                // If the pax doesn't exist, create it
                if (sectorPax == null)
                {
                    sectorPax = new PaxSectorData { PaxName = pax.PaxName, PaxRegionData = new Dictionary<string, PaxRegionData> { { region.Key.DisplayName, pax } } };
                    paxSectorData.Add(sectorPax);
                }
                else
                {
                    // Add the region data to the pax
                    sectorPax.PaxRegionData.Add(region.Key.DisplayName, pax);
                }

                sectorPax.TotalPostCount += pax.PostCount;
                sectorPax.TotalQCount += pax.QCount;
                sectorPax.FirstPost = sectorPax.FirstPost == DateTime.MinValue ? pax.FirstPost : (pax.FirstPost < sectorPax.FirstPost ? pax.FirstPost : sectorPax.FirstPost);

                // Sort the region data by post count
                sectorPax.PaxRegionData = sectorPax.PaxRegionData.OrderByDescending(x => x.Value.PostCount).ToDictionary(x => x.Key, x => x.Value);
            }
        }

        paxSectorData = paxSectorData.OrderByDescending(x => x.TotalPostCount).ToList();

        var allAos = allRegionDataDict.SelectMany(x => x.Value.Aos).Select(x => x.Name).ToList();

        var rtn = new SectorData
        {
            PaxSectorData = paxSectorData,
            TotalPosts = paxSectorData.Sum(x => x.TotalPostCount),
            TotalPax = paxSectorData.Count,
            ActiveLocations = allAos.Count()        ,
            TotalPax30Days = allRegionDataDict
                .SelectMany(x => x.Value.Posts)
                .Where(y => y.Date >= DateTime.Now.AddDays(-30))
                .Select(y => y.Pax)
                .Distinct()
                .Count(),
        };

        // Save to cache
        await CacheHelper.SetCachedDataAsync("SacSector", CacheKeyType.SectorData, JsonSerializer.Serialize(rtn));

        return rtn;
    }

    private async Task<List<TerracottaChallenge>> GetTerracottaChallengeAsync(SheetsService sheetsService)
    {
        try
        {
            var terracottaRegion = RegionList.GetRegion("terracotta");

            var sheetRange = "PAX Data!A2:O";
            var sheetData = await sheetsService.Spreadsheets.Values.Get(terracottaRegion.SpreadsheetId, sheetRange).ExecuteAsync();

            var challenges = sheetData.Values.Select(row => new TerracottaChallenge
            {
                PaxName = row.Count > 0 ? row[0]?.ToString() : string.Empty, // Column A
                ColdPlunges = row.Count > 8 && int.TryParse(row[8]?.ToString(), out var coldPlunges) ? coldPlunges : 0, // Column I
                Volunteers = row.Count > 9 && int.TryParse(row[9]?.ToString(), out var volunteers) ? volunteers : 0, // Column J
                DownrangePosts = row.Count > 12 && int.TryParse(row[12]?.ToString(), out var downrangePosts) ? downrangePosts : 0, // Column M
                RegionCount = row.Count > 14 && int.TryParse(row[14]?.ToString(), out var regionCount) ? regionCount : 0 // Column O
            }).Where(x => !string.IsNullOrEmpty(x.PaxName)).ToList();

            return challenges;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error in GetTerracottaChallengeAsync: {ex.Message}");
            throw;
        }
    }

    private async Task<int> GetSheetRowCountAsync(SheetsService sheetsService, string spreadsheetId, string range)
    {
        var valueRange = await sheetsService.Spreadsheets.Values.Get(spreadsheetId, range).ExecuteAsync();
        int numRows = valueRange.Values.Count;

        return numRows;
    }

    private static SheetsService GetSheetsService()
    {
        ServiceAccountCredential? credentials;
        var svcactJson = Environment.GetEnvironmentVariable("GOOGLE_SVC_ACT_JSON");
        var scopes = new[] { SheetsService.Scope.Spreadsheets };

        if (!string.IsNullOrEmpty(svcactJson))
        {
            credentials = GoogleCredential.FromJson(svcactJson)
                        .CreateScoped(scopes)
                        .UnderlyingCredential as ServiceAccountCredential;
        }
        else
        {
            Console.WriteLine("No Google Service Account credentials found in Env Var. Checking for local file.");

            try
            {
                using (var stream = new FileStream($"Secrets/SvcAct.json", FileMode.Open, FileAccess.Read))
                {
                    // We may be local testing so check for a local file
                    credentials = GoogleCredential.FromStream(stream)
                           .CreateScoped(scopes)
                           .UnderlyingCredential as ServiceAccountCredential;
                }
            }
            catch
            {
                throw new Exception("Unable to get Google Service Account credentials from Env Var or Local File. If you are debugging locally, ensure a valid Secrets/SvcAct.json file exists.");
            }           
        }

        // Create the service.
        var sheetsService = new SheetsService(new BaseClientService.Initializer
        {
            ApplicationName = "F3PaxToSheets",
            HttpClientInitializer = credentials
        });

        return sheetsService;
    }
}