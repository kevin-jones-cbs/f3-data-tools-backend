using System.Collections.Concurrent;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using F3Core;
using F3Core.Regions;
using F3Lambda.Data;
using Google.Apis.Sheets.v4;

namespace F3Lambda;

public sealed class RegionConfigEditorService
{
    public const string SessionSecretEnvironmentVariable = "REGION_CONFIG_SESSION_SECRET";
    public const string GoogleClientIdEnvironmentVariable = "GOOGLE_REGION_EDITOR_CLIENT_ID";
    public const string GooglePickerApiKeyEnvironmentVariable = "GOOGLE_PICKER_API_KEY";

    private static readonly Regex SlugPattern = new("^[a-z0-9-]+$", RegexOptions.Compiled);
    private static readonly Regex SpreadsheetIdPattern = new("^[A-Za-z0-9_-]{20,}$", RegexOptions.Compiled);
    private static readonly ConcurrentDictionary<string, Queue<DateTime>> RateLimitRequests = new();
    private static readonly TimeSpan SessionDuration = TimeSpan.FromMinutes(15);

    private readonly IRegionConfigStore? store;
    private readonly GoogleDrivePermissionService googleDrive;

    public RegionConfigEditorService(IRegionConfigStore? store, GoogleDrivePermissionService? googleDrive = null)
    {
        this.store = store;
        this.googleDrive = googleDrive ?? new GoogleDrivePermissionService();
    }

    public async Task<BeginRegionConfigEditResult> BeginAsync(string slug)
    {
        var snapshot = await GetEditableSnapshotAsync();
        var region = FindRegion(snapshot.Catalog, slug);
        return new BeginRegionConfigEditResult
        {
            Region = RegionMetadata.FromRegion(new ConfiguredRegion(region)),
            GoogleClientId = Environment.GetEnvironmentVariable(GoogleClientIdEnvironmentVariable) ?? string.Empty,
            GooglePickerApiKey = Environment.GetEnvironmentVariable(GooglePickerApiKeyEnvironmentVariable) ?? string.Empty
        };
    }

    public async Task<List<RegionMetadata>> GetRegionIdentitiesAsync()
    {
        var snapshot = await GetEditableSnapshotAsync();
        return snapshot.Catalog.Regions
            .Select(x => RegionMetadata.FromRegion(new ConfiguredRegion(x)))
            .OrderBy(x => x.DisplayName)
            .ToList();
    }

    public async Task<CompleteRegionEditAuthorizationResult> CompleteAuthorizationAsync(
        RegionConfigEditorRequest request)
    {
        EnforceRateLimit($"region:{request.OriginalRegion}", "authorize-attempt", 60);
        var snapshot = await GetEditableSnapshotAsync();
        var region = FindRegion(snapshot.Catalog, request.OriginalRegion);
        if (!string.Equals(region.SpreadsheetId, request.SelectedSpreadsheetId, StringComparison.Ordinal))
        {
            throw new UnauthorizedAccessException("The selected spreadsheet is not the region's current configured spreadsheet.");
        }

        var actor = await googleDrive.VerifyEditorAsync(request.GoogleAccessToken, region.SpreadsheetId);
        EnforceRateLimit(actor.Subject, "authorize", 10);
        var expires = DateTime.UtcNow.Add(SessionDuration);
        var session = new EditSessionPayload
        {
            Subject = actor.Subject,
            Email = actor.Email,
            OriginalRegion = region.QueryStringValue,
            SpreadsheetId = region.SpreadsheetId,
            ETag = snapshot.ETag,
            ExpiresUtc = expires
        };

        return new CompleteRegionEditAuthorizationResult
        {
            SessionToken = ProtectSession(session),
            ExpiresUtc = expires,
            GoogleEmail = actor.Email,
            GoogleSubject = actor.Subject,
            RegionConfig = Clone(region),
            CatalogVersion = snapshot.Catalog.ConfigVersion,
            ETag = snapshot.ETag,
            S3VersionId = snapshot.VersionId,
            LastModifiedUtc = snapshot.LastModifiedUtc,
            Source = snapshot.Source
        };
    }

    public async Task<SpreadsheetInspectionResult> InspectAsync(
        RegionConfigEditorRequest request,
        SheetsService sheetsService)
    {
        var session = UnprotectSession(request.SessionToken);
        EnsureRequestMatchesSession(request, session);
        EnforceRateLimit(session.Subject, "inspect", 30);
        var spreadsheetId = string.IsNullOrWhiteSpace(request.SelectedSpreadsheetId)
            ? session.SpreadsheetId
            : request.SelectedSpreadsheetId;
        var actor = await googleDrive.VerifyEditorAsync(request.GoogleAccessToken, spreadsheetId);
        EnsureSameActor(session, actor);

        var inspection = await OnboardingService.InspectSpreadsheetAsync(sheetsService, spreadsheetId);
        inspection.UserCanEdit = true;
        inspection.IsReplacement = !string.Equals(spreadsheetId, session.SpreadsheetId, StringComparison.Ordinal);
        return inspection;
    }

    public async Task<SheetSchemaPreviewResult> PreviewAsync(
        RegionConfigEditorRequest request,
        SheetsService sheetsService)
    {
        var session = UnprotectSession(request.SessionToken);
        EnsureRequestMatchesSession(request, session);
        EnforceRateLimit(session.Subject, "preview", 60);
        var spreadsheetId = string.IsNullOrWhiteSpace(request.SelectedSpreadsheetId)
            ? session.SpreadsheetId
            : request.SelectedSpreadsheetId;
        var actor = await googleDrive.VerifyEditorAsync(request.GoogleAccessToken, spreadsheetId);
        EnsureSameActor(session, actor);
        return await OnboardingService.GetSheetSchemaPreviewAsync(
            sheetsService,
            spreadsheetId,
            request.SheetId,
            request.SheetName,
            Math.Clamp(request.StartRow, 1, 100_000),
            Math.Clamp(request.MaxRows, 1, 25),
            request.MaxColumns is null ? null : Math.Clamp(request.MaxColumns.Value, 1, 100));
    }

    public async Task<RegionConfigValidationResult> ValidateAsync(
        RegionConfigEditorRequest request,
        SheetsService sheetsService)
    {
        var session = UnprotectSession(request.SessionToken);
        EnsureRequestMatchesSession(request, session);
        EnforceRateLimit(session.Subject, "validate", 20);
        var snapshot = await GetEditableSnapshotAsync();
        EnsureSnapshotMatchesSession(snapshot, session, request.ExpectedETag);
        var current = FindRegion(snapshot.Catalog, session.OriginalRegion);
        EnsureAuthorizationAnchor(current, session);
        var actor = await googleDrive.VerifyEditorAsync(request.GoogleAccessToken, current.SpreadsheetId);
        EnsureSameActor(session, actor);
        return await ValidateCandidateAsync(snapshot.Catalog, session, request.Candidate, request.GoogleAccessToken, sheetsService);
    }

    public async Task<SaveRegionConfigResult> SaveAsync(
        RegionConfigEditorRequest request,
        SheetsService sheetsService)
    {
        var session = UnprotectSession(request.SessionToken);
        EnsureRequestMatchesSession(request, session);
        EnforceRateLimit(session.Subject, "save", 10);
        var snapshot = await GetEditableSnapshotAsync();
        EnsureSnapshotMatchesSession(snapshot, session, request.ExpectedETag);
        var current = FindRegion(snapshot.Catalog, session.OriginalRegion);
        EnsureAuthorizationAnchor(current, session);

        var actor = await googleDrive.VerifyEditorAsync(request.GoogleAccessToken, current.SpreadsheetId);
        EnsureSameActor(session, actor);
        var validation = await ValidateCandidateAsync(
            snapshot.Catalog,
            session,
            request.Candidate,
            request.GoogleAccessToken,
            sheetsService);
        if (!validation.IsValid || !string.Equals(validation.ValidationHash, request.ValidationHash, StringComparison.Ordinal))
        {
            Audit("rejected", session, snapshot, null, validation, Array.Empty<string>());
            throw new InvalidOperationException("The draft must pass the latest authoritative validation before it can be saved.");
        }

        var candidate = Clone(request.Candidate!);
        var changedFields = RegionConfigDiff.GetChangedFields(current, candidate);
        var replacementCatalog = RegionConfigJson.Deserialize(RegionConfigJson.Serialize(snapshot.Catalog));
        var index = replacementCatalog.Regions.FindIndex(x =>
            string.Equals(x.QueryStringValue, session.OriginalRegion, StringComparison.OrdinalIgnoreCase));
        replacementCatalog.Regions[index] = candidate;
        replacementCatalog.SchemaVersion = RegionConfigCatalog.CurrentSchemaVersion;
        replacementCatalog.ConfigVersion = DateTime.UtcNow.ToString("O");

        var save = await store!.SaveCatalogAsync(replacementCatalog, snapshot.ETag);
        S3RegionConfigProvider.ReplaceCachedCatalog(replacementCatalog, S3RegionConfigProvider.GetCacheSourceKey(store));
        await ClearAffectedCachesAsync(current, candidate);
        Audit("saved", session, snapshot, save, validation, changedFields);

        var renewedSession = new EditSessionPayload
        {
            Subject = session.Subject,
            Email = session.Email,
            OriginalRegion = candidate.QueryStringValue,
            SpreadsheetId = candidate.SpreadsheetId,
            ETag = save.ETag,
            ExpiresUtc = DateTime.UtcNow.Add(SessionDuration)
        };

        return new SaveRegionConfigResult
        {
            SessionToken = ProtectSession(renewedSession),
            ExpiresUtc = renewedSession.ExpiresUtc,
            CatalogVersion = replacementCatalog.ConfigVersion,
            ETag = save.ETag,
            S3VersionId = save.VersionId,
            ChangedFields = changedFields,
            Message = "Saved. All Lambda instances should refresh within about two minutes."
        };
    }

    public async Task<RegionConfigLiveVersionResult> GetLiveVersionAsync(RegionConfigEditorRequest request)
    {
        var session = UnprotectSession(request.SessionToken);
        EnsureRequestMatchesSession(request, session);
        var snapshot = await GetEditableSnapshotAsync();
        return new RegionConfigLiveVersionResult
        {
            CatalogVersion = snapshot.Catalog.ConfigVersion,
            ETag = snapshot.ETag,
            VersionId = snapshot.VersionId,
            Source = snapshot.Source
        };
    }

    private async Task<RegionConfigValidationResult> ValidateCandidateAsync(
        RegionConfigCatalog catalog,
        EditSessionPayload session,
        RegionConfig? candidate,
        string googleAccessToken,
        SheetsService sheetsService)
    {
        var result = new RegionConfigValidationResult { ValidatedUtc = DateTime.UtcNow };
        if (candidate == null)
        {
            AddError(result, "regionConfig", "Required", "A candidate region configuration is required.");
            return result;
        }

        ValidateStructure(catalog, session.OriginalRegion, candidate, result);
        if (result.Errors.Count > 0)
        {
            result.ValidationHash = ComputeValidationHash(candidate, session.SpreadsheetId);
            return result;
        }

        if (!string.Equals(candidate.SpreadsheetId, session.SpreadsheetId, StringComparison.Ordinal))
        {
            var replacementActor = await googleDrive.VerifyEditorAsync(googleAccessToken, candidate.SpreadsheetId);
            EnsureSameActor(session, replacementActor);
        }

        SpreadsheetInspectionResult inspection;
        try
        {
            inspection = await OnboardingService.InspectSpreadsheetAsync(sheetsService, candidate.SpreadsheetId);
        }
        catch (Exception ex)
        {
            AddError(result, "spreadsheetId", "ServiceAccountAccessDenied", ex.Message);
            result.ValidationHash = ComputeValidationHash(candidate, candidate.SpreadsheetId);
            return result;
        }

        ValidateTabsAndColumns(candidate, inspection, result);
        await ValidateSamplesAsync(candidate, inspection, sheetsService, result);
        result.ValidationHash = ComputeValidationHash(candidate, candidate.SpreadsheetId);
        return result;
    }

    private static void ValidateStructure(
        RegionConfigCatalog catalog,
        string originalRegion,
        RegionConfig candidate,
        RegionConfigValidationResult result)
    {
        candidate.QueryStringValue = candidate.QueryStringValue.Trim();
        candidate.DisplayName = candidate.DisplayName.Trim();
        candidate.SpreadsheetId = candidate.SpreadsheetId.Trim();
        candidate.RosterSheetName = candidate.RosterSheetName.Trim();
        candidate.AosSheetName = candidate.AosSheetName.Trim();
        candidate.RosterNameColumn = candidate.RosterNameColumn.Trim();

        if (!SlugPattern.IsMatch(candidate.QueryStringValue))
            AddError(result, "queryStringValue", "InvalidSlug", "Use lowercase letters, numbers, and hyphens only.");
        if (catalog.Regions.Any(x =>
            !string.Equals(x.QueryStringValue, originalRegion, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(x.QueryStringValue, candidate.QueryStringValue, StringComparison.OrdinalIgnoreCase)))
            AddError(result, "queryStringValue", "DuplicateSlug", "Another region already uses this slug.");
        if (string.IsNullOrWhiteSpace(candidate.DisplayName))
            AddError(result, "displayName", "Required", "Display name is required.");
        if (catalog.Regions.Any(x =>
            !string.Equals(x.QueryStringValue, originalRegion, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(x.DisplayName, candidate.DisplayName, StringComparison.OrdinalIgnoreCase)))
            AddWarning(result, "displayName", "DuplicateDisplayName", "Another region uses this display name.");
        if (!SpreadsheetIdPattern.IsMatch(candidate.SpreadsheetId))
            AddError(result, "spreadsheetId", "InvalidSpreadsheetId", "Select a valid Google Sheets spreadsheet.");
        if (candidate.MasterDataSheetIds.Count == 0 ||
            candidate.MasterDataSheetIds.Count != candidate.MasterDataSheetNames.Count)
            AddError(result, "masterDataSheetIds", "UnpairedMasterTabs", "Master Data tabs must have paired IDs and names.");
        if (candidate.MissingDataRowOffset < 0 || candidate.MissingDataRowOffset > 1_000_000)
            AddError(result, "missingDataRowOffset", "OutOfRange", "The offset must be between 0 and 1,000,000.");
        if (candidate.RosterSheetColumns.Count(x => x == RosterSheetColumn.PaxName) != 1)
            AddError(result, "rosterSheetColumns", "PaxNameRoleRequired", "Roster roles must contain exactly one PaxName.");
        if (!Regex.IsMatch(candidate.RosterNameColumn, "^[A-Z]+$"))
            AddError(result, "rosterNameColumn", "InvalidColumn", "Select a valid roster name column.");
        if (candidate.HasQSourcePosts &&
            (candidate.MasterDataColumnIndicies.QSourcePost is null || candidate.MasterDataColumnIndicies.QSourceQ is null))
            AddError(result, "hasQSourcePosts", "QSourceMappingsRequired", "Q Source post and Q columns are required when this feature is enabled.");
        if (candidate.HasExtraActivity && candidate.MasterDataColumnIndicies.ExtraActivity is null)
            AddError(result, "hasExtraActivity", "ExtraActivityMappingRequired", "An extra-activity column is required when this feature is enabled.");
    }

    private static void ValidateTabsAndColumns(
        RegionConfig candidate,
        SpreadsheetInspectionResult inspection,
        RegionConfigValidationResult result)
    {
        for (var i = 0; i < candidate.MasterDataSheetIds.Count; i++)
        {
            var tab = inspection.Tabs.FirstOrDefault(x => x.SheetId == candidate.MasterDataSheetIds[i]);
            if (tab == null || !string.Equals(tab.Title, candidate.MasterDataSheetNames[i], StringComparison.Ordinal))
                AddError(result, $"masterDataSheetIds[{i}]", "TabNotFound", "The configured Master Data tab ID/name pair was not found.");
        }

        var roster = inspection.Tabs.FirstOrDefault(x => x.SheetId == candidate.RosterSheetId);
        if (roster == null || !string.Equals(roster.Title, candidate.RosterSheetName, StringComparison.Ordinal))
            AddError(result, "rosterSheetId", "TabNotFound", "The configured roster tab ID/name pair was not found.");
        var aos = inspection.Tabs.FirstOrDefault(x => string.Equals(x.Title, candidate.AosSheetName, StringComparison.Ordinal));
        if (aos == null)
            AddError(result, "aosSheetName", "TabNotFound", "The configured AO tab was not found.");

        var master = inspection.Tabs.FirstOrDefault(x => x.SheetId == candidate.MasterDataSheetIds.LastOrDefault());
        if (master != null)
        {
            CheckColumn(result, "masterDataColumnIndicies.date", candidate.MasterDataColumnIndicies.Date, master.ColumnCount, false);
            CheckColumn(result, "masterDataColumnIndicies.location", candidate.MasterDataColumnIndicies.Location, master.ColumnCount, false);
            CheckColumn(result, "masterDataColumnIndicies.paxName", candidate.MasterDataColumnIndicies.PaxName, master.ColumnCount, false);
            CheckColumn(result, "masterDataColumnIndicies.fng", candidate.MasterDataColumnIndicies.Fng, master.ColumnCount, false);
            CheckColumn(result, "masterDataColumnIndicies.post", candidate.MasterDataColumnIndicies.Post, master.ColumnCount, false);
            CheckColumn(result, "masterDataColumnIndicies.q", candidate.MasterDataColumnIndicies.Q, master.ColumnCount, false);
            CheckColumn(result, "masterDataColumnIndicies.qSourcePost", candidate.MasterDataColumnIndicies.QSourcePost, master.ColumnCount, true);
            CheckColumn(result, "masterDataColumnIndicies.qSourceQ", candidate.MasterDataColumnIndicies.QSourceQ, master.ColumnCount, true);
            CheckColumn(result, "masterDataColumnIndicies.extraActivity", candidate.MasterDataColumnIndicies.ExtraActivity, master.ColumnCount, true);
        }
        if (aos != null)
        {
            CheckColumn(result, "aoColumnIndicies.name", candidate.AoColumnIndicies.Name, aos.ColumnCount, false);
            CheckColumn(result, "aoColumnIndicies.city", candidate.AoColumnIndicies.City, aos.ColumnCount, false);
            CheckColumn(result, "aoColumnIndicies.dayOfWeek", candidate.AoColumnIndicies.DayOfWeek, aos.ColumnCount, false);
            CheckColumn(result, "aoColumnIndicies.retired", candidate.AoColumnIndicies.Retired, aos.ColumnCount, true);
            CheckColumn(result, "aoColumnIndicies.hasQSource", candidate.AoColumnIndicies.HasQSource, aos.ColumnCount, true);
            CheckColumn(result, "aoColumnIndicies.isQSourceOnly", candidate.AoColumnIndicies.IsQSourceOnly, aos.ColumnCount, true);
        }
        if (roster != null && candidate.RosterSheetColumns.Count > roster.ColumnCount)
            AddError(result, "rosterSheetColumns", "ColumnOutOfRange", "Roster roles extend beyond the tab's configured column count.");
    }

    private static async Task ValidateSamplesAsync(
        RegionConfig candidate,
        SpreadsheetInspectionResult inspection,
        SheetsService sheetsService,
        RegionConfigValidationResult result)
    {
        if (result.Errors.Count > 0) return;
        var master = inspection.Tabs.First(x => x.SheetId == candidate.MasterDataSheetIds[^1]);
        var masterPreview = await OnboardingService.GetSheetSchemaPreviewAsync(
            sheetsService, candidate.SpreadsheetId, master.SheetId, master.Title, 1, 20, null);
        var dateSamples = masterPreview.Columns.FirstOrDefault(x => x.Index == candidate.MasterDataColumnIndicies.Date)?.Samples ?? new();
        if (dateSamples.Count == 0 || !dateSamples.Any(x => DateTime.TryParse(x, out _)))
            AddError(result, "masterDataColumnIndicies.date", "DateSamplesInvalid", "The selected column did not contain parseable dates in the sampled rows.");
        if (!(masterPreview.Columns.FirstOrDefault(x => x.Index == candidate.MasterDataColumnIndicies.PaxName)?.Samples.Any() ?? false))
            AddError(result, "masterDataColumnIndicies.paxName", "PaxSamplesEmpty", "The selected PAX column had no non-empty sampled values.");
        if (!(masterPreview.Columns.FirstOrDefault(x => x.Index == candidate.MasterDataColumnIndicies.Location)?.Samples.Any() ?? false))
            AddError(result, "masterDataColumnIndicies.location", "LocationSamplesEmpty", "The selected location column had no non-empty sampled values.");

        result.Counts["masterDataRows"] = Math.Max(0, master.RowCount - 1);
        var roster = inspection.Tabs.First(x => x.SheetId == candidate.RosterSheetId);
        var aos = inspection.Tabs.First(x => x.Title == candidate.AosSheetName);
        var rosterPreview = await OnboardingService.GetSheetSchemaPreviewAsync(
            sheetsService, candidate.SpreadsheetId, roster.SheetId, roster.Title, 1, 20, null);
        var rosterNameIndex = OnboardingService.GetColumnIndex(candidate.RosterNameColumn);
        if (rosterNameIndex < 0 ||
            !(rosterPreview.Columns.FirstOrDefault(x => x.Index == rosterNameIndex)?.Samples.Any() ?? false))
            AddError(result, "rosterNameColumn", "RosterNamesEmpty", "The selected roster name column had no non-empty sampled values.");

        var aoPreview = await OnboardingService.GetSheetSchemaPreviewAsync(
            sheetsService, candidate.SpreadsheetId, aos.SheetId, aos.Title, 1, 20, null);
        if (!(aoPreview.Columns.FirstOrDefault(x => x.Index == candidate.AoColumnIndicies.Name)?.Samples.Any() ?? false))
            AddError(result, "aoColumnIndicies.name", "AoNamesEmpty", "The selected AO name column had no non-empty sampled values.");
        var daySamples = aoPreview.Columns.FirstOrDefault(x => x.Index == candidate.AoColumnIndicies.DayOfWeek)?.Samples ?? new();
        if (daySamples.Count == 0 || !daySamples.Any(x => Enum.TryParse<DayOfWeek>(x, true, out _)))
            AddError(result, "aoColumnIndicies.dayOfWeek", "DayOfWeekSamplesInvalid", "The selected column did not contain recognizable weekdays in the sampled rows.");
        if (candidate.AoColumnIndicies.Retired is short retiredIndex)
        {
            var activeSampleCount = aoPreview.Rows.Count(row =>
                retiredIndex >= row.Count || string.Equals(row[retiredIndex], candidate.AosRetiredIndicator, StringComparison.Ordinal));
            if (activeSampleCount == 0)
                AddWarning(result, "aosRetiredIndicator", "AllAoSamplesFiltered", "The configured status value filtered every sampled AO row.");
        }
        result.Counts["rosterRows"] = Math.Max(0, roster.RowCount - 1);
        result.Counts["aoRows"] = Math.Max(0, aos.RowCount - 1);
    }

    private static void CheckColumn(
        RegionConfigValidationResult result,
        string field,
        short? index,
        int columnCount,
        bool optional)
    {
        if (index == null && optional) return;
        if (index == null || index < 0 || index >= columnCount)
            AddError(result, field, "ColumnOutOfRange", $"Select a column between A and {OnboardingService.GetColumnLetter(Math.Max(0, columnCount - 1))}.");
    }

    private async Task<RegionCatalogSnapshot> GetEditableSnapshotAsync()
    {
        if (store == null)
            throw new InvalidOperationException("The editor cannot use the hard-coded fallback catalog. Configure an S3 or explicit local file store.");
        var snapshot = await store.GetCatalogAsync();
        if (snapshot.Source.StartsWith("file:", StringComparison.Ordinal) && !snapshot.CanWrite)
        {
            // Read and validation remain available locally; SaveCatalogAsync enforces the write switch.
            return snapshot;
        }
        return snapshot;
    }

    private static RegionConfig FindRegion(RegionConfigCatalog catalog, string slug) =>
        catalog.Regions.SingleOrDefault(x => string.Equals(x.QueryStringValue, slug, StringComparison.OrdinalIgnoreCase))
        ?? throw new KeyNotFoundException($"Region '{slug}' was not found.");

    private static void EnsureSnapshotMatchesSession(
        RegionCatalogSnapshot snapshot,
        EditSessionPayload session,
        string expectedETag)
    {
        if (!string.Equals(snapshot.ETag, session.ETag, StringComparison.Ordinal) ||
            !string.Equals(snapshot.ETag, expectedETag, StringComparison.Ordinal))
            throw new RegionConfigConflictException();
    }

    private static void EnsureAuthorizationAnchor(RegionConfig current, EditSessionPayload session)
    {
        if (!string.Equals(current.SpreadsheetId, session.SpreadsheetId, StringComparison.Ordinal))
            throw new UnauthorizedAccessException("The region's authorization spreadsheet changed. Verify again.");
    }

    private static void EnsureRequestMatchesSession(RegionConfigEditorRequest request, EditSessionPayload session)
    {
        if (!string.IsNullOrWhiteSpace(request.OriginalRegion) &&
            !string.Equals(request.OriginalRegion, session.OriginalRegion, StringComparison.OrdinalIgnoreCase))
            throw new UnauthorizedAccessException("This edit session belongs to a different region.");
    }

    private static void EnsureSameActor(EditSessionPayload session, GoogleActor actor)
    {
        if (!string.Equals(session.Subject, actor.Subject, StringComparison.Ordinal))
            throw new UnauthorizedAccessException("The Google credential belongs to a different user.");
    }

    private static string ProtectSession(EditSessionPayload session)
    {
        var secret = GetSessionSecret();
        var payload = Base64Url(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(session)));
        var signature = Base64Url(HMACSHA256.HashData(secret, Encoding.UTF8.GetBytes(payload)));
        return $"{payload}.{signature}";
    }

    private static EditSessionPayload UnprotectSession(string token)
    {
        var parts = token.Split('.');
        if (parts.Length != 2) throw new UnauthorizedAccessException("The edit session is invalid.");
        var expected = HMACSHA256.HashData(GetSessionSecret(), Encoding.UTF8.GetBytes(parts[0]));
        var actual = FromBase64Url(parts[1]);
        if (!CryptographicOperations.FixedTimeEquals(expected, actual))
            throw new UnauthorizedAccessException("The edit session is invalid.");
        var session = JsonSerializer.Deserialize<EditSessionPayload>(Encoding.UTF8.GetString(FromBase64Url(parts[0])))
            ?? throw new UnauthorizedAccessException("The edit session is invalid.");
        if (session.ExpiresUtc <= DateTime.UtcNow)
            throw new UnauthorizedAccessException("The edit session expired. Verify with Google again.");
        return session;
    }

    private static byte[] GetSessionSecret()
    {
        var value = Environment.GetEnvironmentVariable(SessionSecretEnvironmentVariable);
        if (string.IsNullOrWhiteSpace(value) || value.Length < 32)
            throw new InvalidOperationException($"{SessionSecretEnvironmentVariable} must contain at least 32 characters.");
        return Encoding.UTF8.GetBytes(value);
    }

    private static string ComputeValidationHash(RegionConfig candidate, string spreadsheetId)
    {
        var input = $"{spreadsheetId}\n{JsonSerializer.Serialize(candidate, RegionConfigJson.Options)}";
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(input))).ToLowerInvariant();
    }

    private static RegionConfig Clone(RegionConfig config) =>
        JsonSerializer.Deserialize<RegionConfig>(JsonSerializer.Serialize(config, RegionConfigJson.Options), RegionConfigJson.Options)!;

    private static string Base64Url(byte[] value) =>
        Convert.ToBase64String(value).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    private static byte[] FromBase64Url(string value)
    {
        var padded = value.Replace('-', '+').Replace('_', '/');
        padded += new string('=', (4 - padded.Length % 4) % 4);
        return Convert.FromBase64String(padded);
    }

    private static void EnforceRateLimit(string subject, string operation, int maximumPerMinute)
    {
        var queue = RateLimitRequests.GetOrAdd($"{subject}:{operation}", _ => new Queue<DateTime>());
        lock (queue)
        {
            var cutoff = DateTime.UtcNow.AddMinutes(-1);
            while (queue.Count > 0 && queue.Peek() < cutoff) queue.Dequeue();
            if (queue.Count >= maximumPerMinute)
                throw new InvalidOperationException("Too many region editor requests. Wait a moment and try again.");
            queue.Enqueue(DateTime.UtcNow);
        }
    }

    private static async Task ClearAffectedCachesAsync(RegionConfig oldConfig, RegionConfig newConfig)
    {
        await CacheHelper.ClearAllCachedDataAsync(new ConfiguredRegion(oldConfig));
        if (!string.Equals(oldConfig.QueryStringValue, newConfig.QueryStringValue, StringComparison.OrdinalIgnoreCase) ||
            !string.Equals(oldConfig.DisplayName, newConfig.DisplayName, StringComparison.Ordinal))
            await CacheHelper.ClearAllCachedDataAsync(new ConfiguredRegion(newConfig));
        // Sector data is shared and configuration edits are rare; clear it on every save.
        await CacheHelper.ClearSectorDataAsync();
    }

    private static void Audit(
        string outcome,
        EditSessionPayload session,
        RegionCatalogSnapshot previous,
        SaveRegionCatalogResult? saved,
        RegionConfigValidationResult validation,
        IEnumerable<string> changedFields)
    {
        Console.WriteLine(JsonSerializer.Serialize(new
        {
            eventName = "RegionConfigSave",
            outcome,
            actorSubject = session.Subject,
            actorEmail = session.Email,
            authorizationSpreadsheetId = session.SpreadsheetId,
            originalRegion = session.OriginalRegion,
            previousVersion = previous.Catalog.ConfigVersion,
            previousETag = previous.ETag,
            newETag = saved?.ETag,
            newVersionId = saved?.VersionId,
            validationPassed = validation.IsValid,
            changedFields = changedFields.ToArray(),
            timestampUtc = DateTime.UtcNow
        }));
    }

    private static void AddError(RegionConfigValidationResult result, string field, string code, string message) =>
        result.Errors.Add(new RegionConfigValidationMessage { Field = field, Code = code, Message = message });
    private static void AddWarning(RegionConfigValidationResult result, string field, string code, string message) =>
        result.Warnings.Add(new RegionConfigValidationMessage { Field = field, Code = code, Message = message });

    private sealed class EditSessionPayload
    {
        public string Subject { get; set; } = string.Empty;
        public string Email { get; set; } = string.Empty;
        public string OriginalRegion { get; set; } = string.Empty;
        public string SpreadsheetId { get; set; } = string.Empty;
        public string ETag { get; set; } = string.Empty;
        public DateTime ExpiresUtc { get; set; }
    }
}

public sealed class GoogleDrivePermissionService
{
    private const string GoogleSheetsMimeType = "application/vnd.google-apps.spreadsheet";
    private readonly HttpClient httpClient;

    public GoogleDrivePermissionService(HttpClient? httpClient = null)
    {
        this.httpClient = httpClient ?? new HttpClient();
    }

    public async Task<GoogleActor> VerifyEditorAsync(string accessToken, string spreadsheetId)
    {
        if (string.IsNullOrWhiteSpace(accessToken))
            throw new UnauthorizedAccessException("Verify with Google before continuing.");
        using var identityRequest = CreateRequest("https://www.googleapis.com/oauth2/v3/userinfo", accessToken);
        using var identityResponse = await httpClient.SendAsync(identityRequest);
        if (!identityResponse.IsSuccessStatusCode)
            throw new UnauthorizedAccessException("The Google credential expired or is invalid. Verify again.");
        using var identity = JsonDocument.Parse(await identityResponse.Content.ReadAsStringAsync());
        var subject = identity.RootElement.GetProperty("sub").GetString() ?? string.Empty;
        var email = identity.RootElement.TryGetProperty("email", out var emailProperty)
            ? emailProperty.GetString() ?? string.Empty
            : string.Empty;

        var fields = Uri.EscapeDataString("id,name,mimeType,capabilities(canEdit,canModifyContent)");
        using var fileRequest = CreateRequest(
            $"https://www.googleapis.com/drive/v3/files/{Uri.EscapeDataString(spreadsheetId)}?fields={fields}&supportsAllDrives=true",
            accessToken);
        using var fileResponse = await httpClient.SendAsync(fileRequest);
        if (!fileResponse.IsSuccessStatusCode)
            throw new UnauthorizedAccessException("Google did not grant access to the selected spreadsheet.");
        using var file = JsonDocument.Parse(await fileResponse.Content.ReadAsStringAsync());
        var root = file.RootElement;
        var mimeType = root.GetProperty("mimeType").GetString();
        var capabilities = root.GetProperty("capabilities");
        var canEdit = capabilities.TryGetProperty("canEdit", out var canEditProperty) && canEditProperty.GetBoolean();
        var canModify = capabilities.TryGetProperty("canModifyContent", out var canModifyProperty) && canModifyProperty.GetBoolean();
        if (!string.Equals(mimeType, GoogleSheetsMimeType, StringComparison.Ordinal) || !canEdit || !canModify)
            throw new UnauthorizedAccessException("You must be able to edit and modify this Google Sheet.");

        return new GoogleActor(subject, email, root.GetProperty("name").GetString() ?? string.Empty);
    }

    private static HttpRequestMessage CreateRequest(string url, string accessToken)
    {
        var request = new HttpRequestMessage(HttpMethod.Get, url);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
        return request;
    }
}

public sealed record GoogleActor(string Subject, string Email, string SpreadsheetTitle);

internal static class RegionConfigDiff
{
    public static List<string> GetChangedFields(RegionConfig before, RegionConfig after)
    {
        using var left = JsonDocument.Parse(JsonSerializer.Serialize(before, RegionConfigJson.Options));
        using var right = JsonDocument.Parse(JsonSerializer.Serialize(after, RegionConfigJson.Options));
        var changes = new List<string>();
        Compare(left.RootElement, right.RootElement, string.Empty, changes);
        return changes;
    }

    private static void Compare(JsonElement left, JsonElement right, string path, List<string> changes)
    {
        if (left.ValueKind != right.ValueKind)
        {
            changes.Add(path);
            return;
        }
        if (left.ValueKind == JsonValueKind.Object)
        {
            var leftProperties = left.EnumerateObject().ToDictionary(x => x.Name, x => x.Value);
            var rightProperties = right.EnumerateObject().ToDictionary(x => x.Name, x => x.Value);
            foreach (var name in leftProperties.Keys.Union(rightProperties.Keys).OrderBy(x => x))
            {
                if (!leftProperties.TryGetValue(name, out var leftValue) || !rightProperties.TryGetValue(name, out var rightValue))
                    changes.Add(string.IsNullOrEmpty(path) ? name : $"{path}.{name}");
                else
                    Compare(leftValue, rightValue, string.IsNullOrEmpty(path) ? name : $"{path}.{name}", changes);
            }
            return;
        }
        if (left.ValueKind == JsonValueKind.Array)
        {
            if (left.GetRawText() != right.GetRawText()) changes.Add(path);
            return;
        }
        if (left.GetRawText() != right.GetRawText()) changes.Add(path);
    }
}
