using System.Buffers;
using System.Diagnostics;
using System.IO.Compression;
using System.Text;
using System.Text.Json;
using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using F3Lambda.Models;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Services;
using Google.Apis.Sheets.v4;
using Google.Apis.Sheets.v4.Data;
using F3Core;
using F3Core.Regions;
using F3Lambda.Data;
using System.Globalization;
using System.Diagnostics.CodeAnalysis;
using System.IO;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace F3Lambda;

public class Function
{
    private Region region;

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
            region = RegionList.GetRegion(functionInput.Region);

            if (region == null)
            {
                result = "Error, no region specified";
            }

            var sheetsService = GetSheetsService();

            // Get recent posts
            if (functionInput.Action == "GetMissingAos")
            {
                var recentPosts = await GetMissingAosAsync(sheetsService);
                result = recentPosts;
            }

            // Get The Pax
            if (functionInput.Action == "GetPax")
            {
                var paxNames = await GetPaxNamesAsync(sheetsService);
                result = paxNames;
            }

            // Add Pax
            if (functionInput.Action == "AddPax")
            {
                await AddPaxToSheetAsync(sheetsService, functionInput.Pax, functionInput.QDate, functionInput.AoName);
                result = "Pax Added";
            }

            // Get all posts
            if (functionInput.Action == "GetAllPosts")
            {
                var allPosts = await GetAllDataAsync(sheetsService);
                result = allPosts;
            }

            // GetPaxFromComment
            if (functionInput.Action == "GetPaxFromComment")
            {
                var pax = await GetPaxFromCommentAsync(sheetsService, functionInput.Comment);
                result = pax;
            }

            // CheckClose100s
            if (functionInput.Action == "CheckClose100s")
            {
                await CheckClose100sAsync(sheetsService);
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
                var locations = await GetLocationsAsync(sheetsService);
                result = locations;
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

    private async Task CheckClose100sAsync(SheetsService sheetsService)
    {
        var allDataCompressed = await GetAllDataAsync(sheetsService, compress: false);

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
        var last100s = await CacheHelper.GetCachedDataAsync<List<Close100>>(region, CacheKeyType.Close100s);

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
        await CacheHelper.SetCachedDataAsync(region, CacheKeyType.Close100s, serialized);
    }

    private async Task<List<Pax>> GetPaxFromCommentAsync(SheetsService sheetsService, string comment)
    {
        var allPax = await GetPaxNamesAsync(sheetsService);
        var pax = PaxHelper.GetPaxFromComment(comment, allPax);

        return pax;
    }

    private async Task<string> GetAllDataAsync(SheetsService sheetsService, bool compress = true)
    {
        var cacheKeyType = CacheKeyType.AllData;
        var cachedData = await CacheHelper.GetCachedDataAsync<string>(region, cacheKeyType);

        if (cachedData != null && compress)
        {
            return cachedData;
        }

        var valueRange = await sheetsService.Spreadsheets.Values.Get(region.SpreadsheetId, $"{region.MasterDataSheetName}!A2:O").ExecuteAsync();
        var posts = valueRange.Values.Select(x => new Post
        {
            Date = DateTime.Parse(x[region.MasterDataColumnIndicies.Date].ToString()),
            Site = x.Count > region.MasterDataColumnIndicies.Location ? x[region.MasterDataColumnIndicies.Location].ToString() : string.Empty,
            Pax = x.Count > region.MasterDataColumnIndicies.PaxName ? x[region.MasterDataColumnIndicies.PaxName].ToString() : string.Empty,
            IsQ = x.Count > region.MasterDataColumnIndicies.Q ? x[region.MasterDataColumnIndicies.Q].ToString() == "1" : false,
        }).ToList();

        // Get the roster
        var result = await sheetsService.Spreadsheets.Values.Get(region.SpreadsheetId, $"{region.RosterSheetName}!A4:F").ExecuteAsync();

        var paxNameIndex = region.RosterSheetColumns.IndexOf(RosterSheetColumn.PaxName);
        var joinDateIndex = region.RosterSheetColumns.IndexOf(RosterSheetColumn.JoinDate);
        var namingRegionNameIndex = region.RosterSheetColumns.IndexOf(RosterSheetColumn.NamingRegionName) == -1 ? region.RosterSheetColumns.IndexOf(RosterSheetColumn.NamingRegionYN) : region.RosterSheetColumns.IndexOf(RosterSheetColumn.NamingRegionName);

        var pax = result.Values.Select(x => new Pax
        {
            Name = x[paxNameIndex].ToString(),
            DateJoined = x[joinDateIndex].ToString().Contains("/") ? DateTime.Parse(x[joinDateIndex].ToString()).ToShortDateString() : string.Empty,
            NamingRegion = namingRegionNameIndex == -1 ? string.Empty : x[namingRegionNameIndex].ToString()
        }).ToList();

        var rtn = new AllData
        {
            Posts = posts,
            Pax = pax
        };

        var aos = await GetLocationsAsync(sheetsService);
        rtn.Aos = aos;

        // Exclude any "archived" pax
        rtn.Pax = rtn.Pax.Where(x => !x.Name.Contains("(Archived)")).ToList();
        rtn.Posts = rtn.Posts.Where(x => !x.Pax.Contains("(Archived)")).ToList();

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
        await CacheHelper.SetCachedDataAsync(region, cacheKeyType, compressedJson);

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

    private async Task<List<Ao>> GetMissingAosAsync(SheetsService sheetsService)
    {
        try
        {
            var rtn = new List<Ao>();
            var valueRange = await sheetsService.Spreadsheets.Values.Get(region.SpreadsheetId, $"{region.MasterDataSheetName}!A{region.MissingDataRowOffset}:K").ExecuteAsync();

            var dateIndex = region.MasterDataColumnIndicies.Date;
            var aoIndex = region.MasterDataColumnIndicies.Location;
            var qDates = valueRange.Values.Select(x => new { Dates = DateTime.Parse(x[dateIndex].ToString()), AoName = x[aoIndex].ToString() }).ToList();
            var aoList = await GetLocationsAsync(sheetsService);

            // Foreach loop for the last 7 days
            for (int i = 7; i >= 0; i--)
            {
                var date = DateTime.Now.AddDays(-i);
                var dayOfWeek = date.DayOfWeek;

                // Get the AOs for the day of the week
                var aos = aoList.Where(x => x.DayOfWeek == dayOfWeek).ToList();
                foreach (var ao in aos)
                {
                    // Check if there is a post for the date and AO
                    var postExists = qDates.Any(x => x.Dates.Date == date.Date && x.AoName == ao.Name);
                    if (!postExists)
                    {
                        // Add the missing post to the list
                        var missingAo = new Ao
                        {
                            Name = ao.Name,
                            City = ao.City,
                            DayOfWeek = ao.DayOfWeek,
                            Date = date
                        };

                        rtn.Add(missingAo);
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

    private async Task<List<string>> GetPaxNamesAsync(SheetsService sheetsService)
    {
        var result = await sheetsService.Spreadsheets.Values.Get(region.SpreadsheetId, $"{region.RosterSheetName}!{region.RosterNameColumn}:{region.RosterNameColumn}").ExecuteAsync();
        var paxMembers = result.Values.Select(x => x.FirstOrDefault().ToString()).Distinct().ToList();

        // Exclude any "archived" pax
        paxMembers = paxMembers.Where(x => !x.Contains("(Archived)")).ToList();

        return paxMembers;
    }

    private async Task<List<Ao>> GetLocationsAsync(SheetsService sheetsService)
    {
        var cacheKeyType = CacheKeyType.Locations;
        var cachedData = await CacheHelper.GetCachedDataAsync<List<Ao>>(region, cacheKeyType);

        if (cachedData != null)
        {
            return cachedData;
        }

        var result = await sheetsService.Spreadsheets.Values.Get(region.SpreadsheetId, $"{region.AosSheetName}!A2:O").ExecuteAsync();
        var aos = result.Values
            .Where(x => Enum.TryParse(x[region.AoColumnIndicies.DayOfWeek].ToString(), out DayOfWeek _) &&
                       (x.Count <= region.AoColumnIndicies.Retired || x[region.AoColumnIndicies.Retired].ToString() == region.AosRetiredIndicator)) // Ensure it's not retired
            .Select(x =>
            {
                return new Ao
                {
                    Name = x[region.AoColumnIndicies.Name].ToString(),
                    City = x[region.AoColumnIndicies.City].ToString(),
                    DayOfWeek = (DayOfWeek)Enum.Parse(typeof(DayOfWeek), x[region.AoColumnIndicies.DayOfWeek].ToString())
                };
            }).ToList();

        // Save to Cache
        var serialized = JsonSerializer.Serialize(aos);
        await CacheHelper.SetCachedDataAsync(region, cacheKeyType, serialized);

        return aos;
    }

    private async Task AddPaxToSheetAsync(SheetsService sheetsService, List<Pax> pax, DateTime qDate, string ao)
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
        postUpdate.Start.ColumnIndex = region.MasterDataColumnIndicies.Post;
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
        qUpdate.Start.ColumnIndex = region.MasterDataColumnIndicies.Q;
        foreach (var member in pax)
        {
            qUpdate.Rows.Add(new RowData
            {
                Values = new List<CellData> { new CellData { UserEnteredValue = new ExtendedValue { NumberValue = member.IsQ ? 1 : (double?)null } } }
            });
        }

        batchUpdateRequest.Requests.Add(new Request { UpdateCells = qUpdate });

        // Use the Google Sheets API to regex find the text Updated followed by the date and replace the date with the current date. Only search on the Master Data sheet
        var findReplaceRequest = new FindReplaceRequest
        {
            Find = "UPDATED \\d{1,2}\\/\\d{1,2}\\/\\d{4}",
            Replacement = $"UPDATED {DateTime.Now.ToShortDateString()}",
            MatchCase = false,
            MatchEntireCell = true,
            SearchByRegex = true,
            Range = new GridRange
            {
                SheetId = region.MasterDataSheetId,
                StartRowIndex = 0,
                EndRowIndex = int.MaxValue,
                StartColumnIndex = region.MasterDataColumnIndicies.PaxName,
                EndColumnIndex = region.MasterDataColumnIndicies.PaxName + 1
            }
        };

        batchUpdateRequest.Requests.Add(new Request { FindReplace = findReplaceRequest });

        // If there are any fngs, do another UpdateCellsRequest for the roster sheet
        if (pax.Any(x => x.IsFng))
        {
            // Get the number of roster rows
            var rosterCount = await GetSheetRowCountAsync(sheetsService, region.SpreadsheetId, $"{region.RosterSheetName}!A:A");

            var updateFngCellsRequest = new UpdateCellsRequest
            {
                Start = new GridCoordinate
                {
                    SheetId = region.RosterSheetId,
                    RowIndex = rosterCount,
                    ColumnIndex = 1
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

                // Remove the first column.
                rowData.Values.RemoveAt(0);

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
