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
