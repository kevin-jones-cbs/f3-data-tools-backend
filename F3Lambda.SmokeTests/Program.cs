using System.Diagnostics;
using System.IO.Compression;
using System.Text;
using System.Text.Json;
using Amazon.Lambda.APIGatewayEvents;
using F3Core;
using F3Core.Regions;
using F3Lambda;
using F3Lambda.Data;

var options = SmokeOptions.Parse(args);
var results = new List<SmokeResult>();

if (options.ShowHelp)
{
    SmokeOptions.PrintHelp();
    return 0;
}

foreach (var target in options.Targets)
{
    var testCases = BuildTestCases(options, target).ToList();
    Console.WriteLine($"Running {testCases.Count} smoke checks against {target}");
    Console.WriteLine();

    using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(options.TimeoutSeconds) };
    var lambda = target == SmokeTarget.Local ? new Function() : null;

    if (target == SmokeTarget.Local)
    {
        UseLambdaProjectDirectoryForLocalSecrets();
        if (!options.UseCache)
        {
            Environment.SetEnvironmentVariable(CacheHelper.SkipMomentoEnvironmentVariable, "true");
        }

        if (!TryValidateLocalConfiguration(testCases, out var validationError))
        {
            var result = SmokeResult.Fail(target, new SmokeCase("LocalPreflight", null), TimeSpan.Zero, validationError);
            results.Add(result);
            Console.WriteLine(FormatResult(result));
            continue;
        }
    }

    var targetResults = await RunTargetAsync(target, testCases, client, lambda, options);
    results.AddRange(targetResults);

    Console.WriteLine();
}

PrintResults(results);

return results.Any(result => !result.Passed) ? 1 : 0;

static IEnumerable<SmokeCase> BuildTestCases(SmokeOptions options, SmokeTarget target)
{
    var selectedActions = LambdaActions.SmokeTestActions
        .Where(action => options.Actions.Count == 0 || options.Actions.Contains(action.Name, StringComparer.Ordinal))
        .Where(action => target != SmokeTarget.Local || options.IncludeExpensive || options.Actions.Count > 0 || !IsExpensiveLocalAction(action.Name))
        .ToList();

    var selectedRegions = RegionList.All
        .Where(region => options.Regions.Count == 0 || options.Regions.Contains(region.QueryStringValue, StringComparer.OrdinalIgnoreCase))
        .ToList();

    foreach (var action in selectedActions)
    {
        if (!action.RequiresRegion)
        {
            yield return new SmokeCase(action.Name, null);
            continue;
        }

        foreach (var region in selectedRegions)
        {
            yield return new SmokeCase(action.Name, region.QueryStringValue);
        }
    }
}

static bool IsExpensiveLocalAction(string action)
{
    return action is LambdaActions.GetSectorDataSummaryAsync
        or LambdaActions.GetInitialView
        or LambdaActions.GetRegionSummary;
}

static async Task<List<SmokeResult>> RunTargetAsync(
    SmokeTarget target,
    IReadOnlyList<SmokeCase> testCases,
    HttpClient client,
    Function? lambda,
    SmokeOptions options)
{
    var results = new List<SmokeResult>();
    using var throttler = new SemaphoreSlim(options.Concurrency);

    var tasks = testCases.Select(async testCase =>
    {
        await throttler.WaitAsync();
        try
        {
            return await RunCaseAsync(target, testCase, client, lambda, options);
        }
        finally
        {
            throttler.Release();
        }
    });

    var delayMilliseconds = GetDelayMilliseconds(target, options);

    foreach (var task in tasks)
    {
        var result = await task;
        results.Add(result);
        Console.WriteLine(FormatResult(result));

        if (delayMilliseconds > 0)
        {
            await Task.Delay(delayMilliseconds);
        }
    }

    return results;
}

static async Task<SmokeResult> RunCaseAsync(
    SmokeTarget target,
    SmokeCase testCase,
    HttpClient client,
    Function? lambda,
    SmokeOptions options)
{
    var stopwatch = Stopwatch.StartNew();
    var attempt = 0;

    while (true)
    {
        try
        {
            var input = BuildInput(testCase);
            var payload = target == SmokeTarget.Local
                ? await InvokeLocalAsync(lambda ?? throw new InvalidOperationException("Local Lambda is not configured."), input)
                : await InvokeUrlAsync(client, options.DevUrl, input);

            var details = ValidatePayload(testCase, payload);
            stopwatch.Stop();
            return SmokeResult.Pass(target, testCase, stopwatch.Elapsed, attempt == 0 ? details : $"{details} after {attempt + 1} attempts");
        }
        catch (Exception ex) when (attempt < options.Retries && IsRetryable(ex))
        {
            attempt++;
            await Task.Delay(TimeSpan.FromSeconds(options.RetryDelaySeconds));
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            return SmokeResult.Fail(target, testCase, stopwatch.Elapsed, ex.Message);
        }
    }
}

static bool IsRetryable(Exception ex)
{
    return ex.Message.Contains("TooManyRequests", StringComparison.OrdinalIgnoreCase) ||
        ex.Message.Contains("Quota exceeded", StringComparison.OrdinalIgnoreCase) ||
        ex.Message.Contains("HTTP 429", StringComparison.OrdinalIgnoreCase);
}

static int GetDelayMilliseconds(SmokeTarget target, SmokeOptions options)
{
    if (options.DelayMilliseconds >= 0)
    {
        return options.DelayMilliseconds;
    }

    return target == SmokeTarget.Local && !options.UseCache ? 1000 : 0;
}

static FunctionInput BuildInput(SmokeCase testCase)
{
    return new FunctionInput
    {
        Action = testCase.Action,
        Region = testCase.Region ?? string.Empty,
        Comment = "Peacock Clark Mani Pedi"
    };
}

static async Task<string> InvokeLocalAsync(Function lambda, FunctionInput input)
{
    var request = new APIGatewayHttpApiV2ProxyRequest
    {
        Body = JsonSerializer.Serialize(input)
    };

    var result = await lambda.FunctionHandler(request, context: null!);

    if (result is APIGatewayProxyResponse proxyResponse)
    {
        if (proxyResponse.StatusCode < 200 || proxyResponse.StatusCode >= 300)
        {
            throw new InvalidOperationException($"HTTP {proxyResponse.StatusCode}: {proxyResponse.Body}");
        }

        return proxyResponse.Body;
    }

    return result is string value ? value : JsonSerializer.Serialize(result);
}

static async Task<string> InvokeUrlAsync(HttpClient client, string url, FunctionInput input)
{
    var json = JsonSerializer.Serialize(input);
    using var content = new StringContent(json, Encoding.UTF8, "application/json");
    using var response = await client.PostAsync(url, content);
    var payload = await response.Content.ReadAsStringAsync();

    if (!response.IsSuccessStatusCode)
    {
        throw new InvalidOperationException($"HTTP {(int)response.StatusCode}: {payload}");
    }

    return payload;
}

static string ValidatePayload(SmokeCase testCase, string rawPayload)
{
    var payload = NormalizePayload(rawPayload);

    if (string.IsNullOrWhiteSpace(payload))
    {
        throw new InvalidOperationException("Empty response payload.");
    }

    if (payload.StartsWith("Error", StringComparison.OrdinalIgnoreCase))
    {
        throw new InvalidOperationException(payload);
    }

    return testCase.Action switch
    {
        LambdaActions.GetMissingAos => ValidateList<Ao>(payload, allowEmpty: true, "missing AOs"),
        LambdaActions.GetPax => ValidateList<string>(payload, allowEmpty: false, "PAX"),
        LambdaActions.GetLocations => ValidateLocations(payload),
        LambdaActions.GetInitialView => ValidateInitialView(payload),
        LambdaActions.GetRegionSummary => ValidateRegionSummary(payload),
        LambdaActions.GetSectorDataSummaryAsync => ValidateSectorSummary(payload),
        LambdaActions.GetTerracottaChallenge => ValidateList<TerracottaChallenge>(payload, allowEmpty: false, "Terracotta rows"),
        LambdaActions.GetForgeChallenge => ValidateList<ForgeChallenge>(payload, allowEmpty: false, "Forge rows"),
        LambdaActions.GetTowerChallenge => ValidateTowerChallenge(payload),
        _ => "payload returned"
    };
}

static string ValidateList<T>(string payload, bool allowEmpty, string label)
{
    var list = DeserializePayload<List<T>>(payload);
    if (!allowEmpty && list.Count == 0)
    {
        throw new InvalidOperationException($"Expected non-empty {label} list.");
    }

    return $"{list.Count} {label}";
}

static string ValidateLocations(string payload)
{
    var locations = DeserializePayload<List<Ao>>(payload);
    if (locations.Count == 0)
    {
        throw new InvalidOperationException("Expected at least one location.");
    }

    if (locations.Any(location => string.IsNullOrWhiteSpace(location.Name)))
    {
        throw new InvalidOperationException("One or more locations has a blank name.");
    }

    return $"{locations.Count} locations";
}

static string ValidateInitialView(string payload)
{
    var initialView = DeserializePayload<InitialViewData>(payload);
    if (initialView.CurrentRows == null || initialView.CurrentRows.Count == 0)
    {
        throw new InvalidOperationException("Expected InitialView current rows.");
    }

    if (initialView.ValidYears == null || initialView.ValidYears.Count == 0)
    {
        throw new InvalidOperationException("Expected InitialView valid years.");
    }

    return $"{initialView.CurrentRows.Count} rows";
}

static string ValidateRegionSummary(string payload)
{
    var summary = DeserializePayload<RegionSummaryData>(payload);
    if (summary.AoCount <= 0)
    {
        throw new InvalidOperationException("Expected RegionSummary AO count.");
    }

    if (summary.PaxData == null || summary.PaxData.Count == 0)
    {
        throw new InvalidOperationException("Expected RegionSummary PAX data.");
    }

    return $"{summary.AoCount} AOs, {summary.PaxData.Count} PAX";
}

static string ValidateSectorSummary(string payload)
{
    var json = Decompress(payload);
    var sectorData = DeserializePayload<SectorData>(json);

    if (sectorData.TotalPosts <= 0 || sectorData.TotalPax <= 0 || sectorData.ActiveLocations <= 0)
    {
        throw new InvalidOperationException("Expected non-zero sector totals.");
    }

    return $"{sectorData.TotalPosts} posts, {sectorData.ActiveLocations} locations";
}

static string ValidateTowerChallenge(string payload)
{
    var tower = DeserializePayload<TowerChallengeResponse>(payload);
    if (tower.ChallengeData.Count == 0 && tower.ChallengeSites.Count == 0)
    {
        throw new InvalidOperationException("Expected Tower challenge data or sites.");
    }

    return $"{tower.ChallengeData.Count} rows, {tower.ChallengeSites.Count} sites";
}

static T DeserializePayload<T>(string payload)
{
    var normalized = NormalizePayload(payload);
    return JsonSerializer.Deserialize<T>(normalized, new JsonSerializerOptions { PropertyNameCaseInsensitive = true })
        ?? throw new InvalidOperationException($"Unable to deserialize payload as {typeof(T).Name}.");
}

static string NormalizePayload(string payload)
{
    var normalized = payload.Trim();

    for (var i = 0; i < 3 && normalized.StartsWith("\"", StringComparison.Ordinal); i++)
    {
        var unwrapped = JsonSerializer.Deserialize<string>(normalized);
        if (string.IsNullOrWhiteSpace(unwrapped))
        {
            break;
        }

        normalized = unwrapped.Trim();
    }

    return normalized;
}

static string Decompress(string compressedText)
{
    var gZipBuffer = Convert.FromBase64String(NormalizePayload(compressedText));

    using var memoryStream = new MemoryStream();
    var dataLength = BitConverter.ToInt32(gZipBuffer, 0);
    memoryStream.Write(gZipBuffer, 4, gZipBuffer.Length - 4);

    var buffer = new byte[dataLength];
    memoryStream.Position = 0;

    using var gZipStream = new GZipStream(memoryStream, CompressionMode.Decompress);
    var totalRead = 0;
    while (totalRead < buffer.Length)
    {
        var bytesRead = gZipStream.Read(buffer, totalRead, buffer.Length - totalRead);
        if (bytesRead == 0)
        {
            break;
        }

        totalRead += bytesRead;
    }

    return Encoding.UTF8.GetString(buffer);
}

static void UseLambdaProjectDirectoryForLocalSecrets()
{
    var currentDirectory = Directory.GetCurrentDirectory();
    var lambdaProjectDirectory = Path.Combine(currentDirectory, "f3-data-tools-backend", "F3Lambda");

    if (!Directory.Exists(lambdaProjectDirectory))
    {
        lambdaProjectDirectory = Path.Combine(currentDirectory, "F3Lambda");
    }

    if (Directory.Exists(lambdaProjectDirectory))
    {
        Directory.SetCurrentDirectory(lambdaProjectDirectory);
    }
}

static bool TryValidateLocalConfiguration(IEnumerable<SmokeCase> testCases, out string error)
{
    var missing = new List<string>();

    if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("GOOGLE_SVC_ACT_JSON")) &&
        !File.Exists(Path.Combine("Secrets", "SvcAct.json")))
    {
        missing.Add("GOOGLE_SVC_ACT_JSON or F3Lambda/Secrets/SvcAct.json");
    }

    if (testCases.Any(testCase => RequiresMomento(testCase.Action)) &&
        !CacheHelper.ShouldSkipMomento &&
        string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("F3_MOMENTO_TOKEN")))
    {
        missing.Add("F3_MOMENTO_TOKEN");
    }

    if (missing.Count == 0)
    {
        error = string.Empty;
        return true;
    }

    error = "Missing local configuration: " + string.Join(", ", missing);
    return false;
}

static bool RequiresMomento(string action)
{
    return action is LambdaActions.GetMissingAos
        or LambdaActions.GetLocations
        or LambdaActions.GetSectorDataSummaryAsync
        or LambdaActions.GetInitialView
        or LambdaActions.GetRegionSummary;
}

static void PrintResults(IReadOnlyList<SmokeResult> results)
{
    Console.WriteLine();

    var passed = results.Count(result => result.Passed);
    var failed = results.Count - passed;

    Console.WriteLine($"Summary: {passed} passed, {failed} failed");

    if (failed == 0)
    {
        return;
    }

    Console.WriteLine();
    Console.WriteLine("Failures:");
    foreach (var result in results.Where(result => !result.Passed))
    {
        Console.WriteLine($"  {result.Target,-5} {result.Case.Action,-28} {result.Case.Region ?? "-", -12} {result.Details}");
    }
}

static string FormatResult(SmokeResult result)
{
    var status = result.Passed ? "PASS" : "FAIL";
    return $"{status,-4} {result.Target,-5} {result.Case.Action,-28} {result.Case.Region ?? "-", -12} {result.Elapsed.TotalMilliseconds,7:0} ms  {result.Details}";
}

enum SmokeTarget
{
    Local,
    Dev
}

sealed record SmokeCase(string Action, string? Region);

sealed record SmokeResult(SmokeTarget Target, SmokeCase Case, bool Passed, TimeSpan Elapsed, string Details)
{
    public static SmokeResult Pass(SmokeTarget target, SmokeCase testCase, TimeSpan elapsed, string details) =>
        new(target, testCase, true, elapsed, details);

    public static SmokeResult Fail(SmokeTarget target, SmokeCase testCase, TimeSpan elapsed, string details) =>
        new(target, testCase, false, elapsed, details);
}

sealed record SmokeOptions
{
    private const string DefaultDevUrl = "https://s6oww3m3a5svbuxq5pf35pjigu0xxaqk.lambda-url.us-west-1.on.aws/";

    public bool ShowHelp { get; private init; }
    public List<SmokeTarget> Targets { get; private init; } = new() { SmokeTarget.Local };
    public List<string> Regions { get; private init; } = new();
    public List<string> Actions { get; private init; } = new();
    public string DevUrl { get; private init; } = Environment.GetEnvironmentVariable("F3_SMOKE_DEV_URL") ?? DefaultDevUrl;
    public int TimeoutSeconds { get; private init; } = 60;
    public int Concurrency { get; private init; } = 4;
    public int DelayMilliseconds { get; private init; } = -1;
    public int Retries { get; private init; } = 1;
    public int RetryDelaySeconds { get; private init; } = 65;
    public bool UseCache { get; private init; }
    public bool IncludeExpensive { get; private init; }

    public static SmokeOptions Parse(string[] args)
    {
        var options = new SmokeOptions();

        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            switch (arg)
            {
                case "--help":
                case "-h":
                    options = options with { ShowHelp = true };
                    break;
                case "--target":
                    options = options with { Targets = ParseTargets(ReadValue(args, ref i, arg)) };
                    break;
                case "--regions":
                    options = options with { Regions = SplitList(ReadValue(args, ref i, arg)) };
                    break;
                case "--actions":
                    options = options with { Actions = SplitList(ReadValue(args, ref i, arg)) };
                    break;
                case "--url":
                    options = options with { DevUrl = ReadValue(args, ref i, arg) };
                    break;
                case "--timeout":
                    options = options with { TimeoutSeconds = int.Parse(ReadValue(args, ref i, arg)) };
                    break;
                case "--concurrency":
                    options = options with { Concurrency = int.Parse(ReadValue(args, ref i, arg)) };
                    break;
                case "--delay-ms":
                    options = options with { DelayMilliseconds = int.Parse(ReadValue(args, ref i, arg)) };
                    break;
                case "--retries":
                    options = options with { Retries = int.Parse(ReadValue(args, ref i, arg)) };
                    break;
                case "--retry-delay":
                    options = options with { RetryDelaySeconds = int.Parse(ReadValue(args, ref i, arg)) };
                    break;
                case "--use-cache":
                    options = options with { UseCache = true };
                    break;
                case "--include-expensive":
                    options = options with { IncludeExpensive = true };
                    break;
                default:
                    throw new ArgumentException($"Unknown option '{arg}'. Use --help for usage.");
            }
        }

        return options;
    }

    public static void PrintHelp()
    {
        Console.WriteLine("Usage:");
        Console.WriteLine("  dotnet run --project F3Lambda.SmokeTests -- --target local");
        Console.WriteLine("  dotnet run --project F3Lambda.SmokeTests -- --target dev --url https://...");
        Console.WriteLine("  dotnet run --project F3Lambda.SmokeTests -- --target local,dev --regions southfork,rubicon");
        Console.WriteLine();
        Console.WriteLine("Options:");
        Console.WriteLine("  --target       local, dev, or local,dev. Default: local");
        Console.WriteLine("  --regions      Comma-separated region query values. Default: all regions");
        Console.WriteLine("  --actions      Comma-separated action names. Default: LambdaActions.SmokeTestActions");
        Console.WriteLine("  --url          Dev Lambda URL. Default: F3_SMOKE_DEV_URL or current frontend URL");
        Console.WriteLine("  --timeout      Per-request HTTP timeout in seconds. Default: 60");
        Console.WriteLine("  --concurrency  Max concurrent checks. Default: 4");
        Console.WriteLine("  --delay-ms     Delay after each check. Default: 1000 for uncached local, 0 otherwise");
        Console.WriteLine("  --retries      Retry count for quota/rate-limit failures. Default: 1");
        Console.WriteLine("  --retry-delay  Seconds to wait before a retry. Default: 65");
        Console.WriteLine("  --use-cache    Local only: use Momento cache instead of setting F3_SKIP_MOMENTO=true");
        Console.WriteLine("  --include-expensive");
        Console.WriteLine("                 Local only: include uncached InitialView, RegionSummary, and sector aggregate checks");
    }

    private static string ReadValue(string[] args, ref int index, string option)
    {
        if (index + 1 >= args.Length)
        {
            throw new ArgumentException($"Missing value for {option}.");
        }

        index++;
        return args[index];
    }

    private static List<string> SplitList(string value)
    {
        return value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
    }

    private static List<SmokeTarget> ParseTargets(string value)
    {
        return SplitList(value)
            .Select(target => Enum.Parse<SmokeTarget>(target, ignoreCase: true))
            .Distinct()
            .ToList();
    }
}
