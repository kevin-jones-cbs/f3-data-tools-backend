using Google.Apis.Sheets.v4;
using Google.Apis.Sheets.v4.Data;

namespace F3Lambda;

/// <summary>
/// Handles onboarding-related operations that work with user-provided spreadsheet IDs
/// rather than pre-configured region configs.
/// </summary>
public static class OnboardingService
{
    /// <summary>
    /// Verify that the service account has access to the specified spreadsheet.
    /// </summary>
    public static async Task<object> VerifySpreadsheetAccessAsync(SheetsService sheetsService, string spreadsheetId)
    {
        if (string.IsNullOrEmpty(spreadsheetId))
        {
            return new VerifyAccessResult
            {
                Success = false,
                Error = "SpreadsheetId is required"
            };
        }

        try
        {
            // Try to get spreadsheet metadata - this will fail if we don't have access
            var request = sheetsService.Spreadsheets.Get(spreadsheetId);
            request.Fields = "properties.title";
            var spreadsheet = await request.ExecuteAsync();

            return new VerifyAccessResult
            {
                Success = true,
                SpreadsheetName = spreadsheet.Properties.Title
            };
        }
        catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return new VerifyAccessResult
            {
                Success = false,
                Error = "Spreadsheet not found. Check that the URL is correct."
            };
        }
        catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.Forbidden)
        {
            return new VerifyAccessResult
            {
                Success = false,
                Error = "Access denied. Make sure you've shared the spreadsheet with the service account as an Editor."
            };
        }
        catch (Exception ex)
        {
            return new VerifyAccessResult
            {
                Success = false,
                Error = $"Failed to access spreadsheet: {ex.Message}"
            };
        }
    }

    /// <summary>
    /// Get the list of sheet tabs from a spreadsheet.
    /// </summary>
    public static async Task<object> GetSheetTabsAsync(SheetsService sheetsService, string spreadsheetId)
    {
        if (string.IsNullOrEmpty(spreadsheetId))
        {
            throw new ArgumentException("SpreadsheetId is required");
        }

        try
        {
            // Get spreadsheet metadata including sheet info
            var request = sheetsService.Spreadsheets.Get(spreadsheetId);
            request.Fields = "properties.title,sheets.properties";
            var spreadsheet = await request.ExecuteAsync();

            var sheets = spreadsheet.Sheets.Select((sheet, index) => new SheetTabInfo
            {
                SheetId = sheet.Properties.SheetId ?? 0,
                Title = sheet.Properties.Title,
                Index = sheet.Properties.Index ?? index
            }).OrderBy(s => s.Index).ToList();

            return sheets;
        }
        catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
        {
            throw new Exception("Spreadsheet not found. Check that the URL is correct.");
        }
        catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.Forbidden)
        {
            throw new Exception("Access denied. Make sure you've shared the spreadsheet with the service account as an Editor.");
        }
    }

    /// <summary>
    /// Get a preview of data from a specific sheet (first few rows).
    /// </summary>
    public static async Task<object> GetSheetPreviewAsync(SheetsService sheetsService, string spreadsheetId, string sheetName, int maxRows = 5)
    {
        if (string.IsNullOrEmpty(spreadsheetId))
        {
            throw new ArgumentException("SpreadsheetId is required");
        }

        if (string.IsNullOrEmpty(sheetName))
        {
            throw new ArgumentException("SheetName is required");
        }

        try
        {
            // Request the first maxRows+1 rows (header + data rows)
            var range = $"'{sheetName}'!1:{maxRows + 1}";
            var request = sheetsService.Spreadsheets.Values.Get(spreadsheetId, range);
            var response = await request.ExecuteAsync();

            var values = response.Values;
            if (values == null || values.Count == 0)
            {
                return new SheetPreviewResult
                {
                    Headers = new List<string>(),
                    Rows = new List<List<string>>(),
                    ColumnCount = 0
                };
            }

            // First row is headers
            var headers = values[0].Select(v => v?.ToString() ?? "").ToList();
            var columnCount = headers.Count;

            // Generate column letters (A, B, C, ... Z, AA, AB, ...)
            var columnLetters = Enumerable.Range(0, columnCount)
                .Select(i => GetColumnLetter(i))
                .ToList();

            // Remaining rows are data
            var rows = values.Skip(1).Select(row =>
            {
                var rowData = new List<string>();
                for (int i = 0; i < columnCount; i++)
                {
                    rowData.Add(i < row.Count ? row[i]?.ToString() ?? "" : "");
                }
                return rowData;
            }).ToList();

            return new SheetPreviewResult
            {
                Headers = headers,
                ColumnLetters = columnLetters,
                Rows = rows,
                ColumnCount = columnCount
            };
        }
        catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
        {
            throw new Exception($"Sheet '{sheetName}' not found in the spreadsheet.");
        }
        catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.Forbidden)
        {
            throw new Exception("Access denied. Make sure you've shared the spreadsheet with the service account as an Editor.");
        }
    }

    /// <summary>
    /// Convert a 0-based column index to a column letter (A, B, ... Z, AA, AB, ...)
    /// </summary>
    private static string GetColumnLetter(int index)
    {
        var result = "";
        while (index >= 0)
        {
            result = (char)('A' + (index % 26)) + result;
            index = index / 26 - 1;
        }
        return result;
    }
}

/// <summary>
/// Result of getting a sheet preview
/// </summary>
public class SheetPreviewResult
{
    public List<string> Headers { get; set; } = new();
    public List<string> ColumnLetters { get; set; } = new();
    public List<List<string>> Rows { get; set; } = new();
    public int ColumnCount { get; set; }
}

/// <summary>
/// Result of verifying spreadsheet access
/// </summary>
public class VerifyAccessResult
{
    public bool Success { get; set; }
    public string? SpreadsheetName { get; set; }
    public string? Error { get; set; }
}

/// <summary>
/// Information about a sheet tab
/// </summary>
public class SheetTabInfo
{
    public int SheetId { get; set; }
    public string Title { get; set; } = string.Empty;
    public int Index { get; set; }
}
