using Amazon.Lambda.APIGatewayEvents;
using Amazon.S3;
using F3Lambda;
using F3Lambda.Data;
using F3Lambda.Analytics;
using System.Text.Json;

var hosted = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("AWS_LAMBDA_FUNCTION_NAME"));
if (!hosted) UseLambdaProjectDirectoryForLocalSecrets();

if (!hosted && string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(CacheHelper.SkipMomentoEnvironmentVariable)))
{
    Environment.SetEnvironmentVariable(CacheHelper.SkipMomentoEnvironmentVariable, "true");
}

if (!hosted && string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(S3RegionConfigProvider.FileEnvironmentVariable)) &&
    File.Exists("regions.json"))
{
    Environment.SetEnvironmentVariable(S3RegionConfigProvider.FileEnvironmentVariable, "regions.json");
}

var builder = WebApplication.CreateBuilder(args);
builder.Configuration
    .AddJsonFile(Path.GetFullPath("Secrets/chat.local.json"), optional: true, reloadOnChange: false)
    .AddEnvironmentVariables(); // Environment overrides local file settings for evals/hosting.
builder.WebHost.ConfigureKestrel(options => options.Limits.MaxRequestBodySize = 6 * 1024 * 1024);
var analyticsPath = builder.Configuration["F3_ANALYTICS_DB_PATH"]
    ?? Path.GetFullPath("../tools/southfork-duckdb/data/southfork.duckdb");
var defaultModel = builder.Configuration["OPENROUTER_MODEL"] ?? "";
var allowedModels = (builder.Configuration["OPENROUTER_ALLOWED_MODELS"] ?? "")
    .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
    .Append(defaultModel).Where(m => m.Length > 0).Distinct().ToArray();
var openRouterKey = builder.Configuration["OPENROUTER_API_KEY"] ?? "";
var regionPaths = builder.Configuration.GetSection("F3_ANALYTICS_REGIONS").GetChildren()
    .ToDictionary(c => ChatRegion.Normalize(c.Key), c => c.Value ?? "");
regionPaths.TryAdd("southfork", analyticsPath);
var duckDbExecutable = builder.Configuration["DUCKDB_EXECUTABLE"] ?? "duckdb";
var telemetryPath = builder.Configuration["F3_CHAT_LOG_DB_PATH"]
    ?? Path.GetFullPath("../tools/chat-telemetry/data/chat-telemetry.duckdb");
if (regionPaths.Values.Any(path => !path.StartsWith("s3://", StringComparison.OrdinalIgnoreCase) &&
    Path.GetFullPath(path) == Path.GetFullPath(telemetryPath)))
    throw new InvalidOperationException("Chat telemetry must use a separate database from the attendance snapshot.");
// A local writable DuckDB is not durable or shared across Lambda environments.
if (!hosted)
{
    builder.Services.AddSingleton(new DuckDbChatTelemetry(telemetryPath,
        duckDbExecutable, builder.Configuration["F3_CHAT_LOG_SOURCE"] ?? "app"));
    builder.Services.AddSingleton<IChatTelemetry>(sp => sp.GetRequiredService<DuckDbChatTelemetry>());
}
var cacheRoot = builder.Configuration["F3_ANALYTICS_CACHE_PATH"]
    ?? Path.Combine(Path.GetTempPath(), "f3-analytics");
AmazonS3Client? s3 = null;
var regionSnapshots = regionPaths.ToDictionary(entry => entry.Key, entry =>
{
    if (!entry.Value.StartsWith("s3://", StringComparison.OrdinalIgnoreCase))
        return (IAnalyticsSnapshotProvider)new LocalAnalyticsSnapshotProvider(entry.Value);
    var uri = new Uri(entry.Value);
    if (uri.Host.Length == 0 || uri.AbsolutePath.Length <= 1 || uri.Query.Length > 0 || uri.Fragment.Length > 0)
        throw new InvalidOperationException("Use an S3 snapshot URI with a bucket and object key, without query or fragment.");
    s3 ??= new AmazonS3Client(); // Lambda execution-role credentials; never passed to DuckDB.
    return new S3AnalyticsSnapshotProvider(s3, uri.Host, Uri.UnescapeDataString(uri.AbsolutePath[1..]),
        Path.Combine(cacheRoot, entry.Key), async (path, ct) =>
        {
            var db = new DuckDbAnalyticsDatabase(new LocalAnalyticsSnapshotProvider(path), duckDbExecutable, entry.Key);
            await db.GetSnapshotAsync(ct); // Validate schema, metadata, and region before publishing.
        });
});
IAnalyticsDatabase DatabaseFor(string region)
{
    region = ChatRegion.Normalize(region);
    return regionSnapshots.TryGetValue(region, out var snapshot)
        ? new DuckDbAnalyticsDatabase(new PinnedAnalyticsSnapshotProvider(snapshot), duckDbExecutable, region)
        : throw new InvalidOperationException($"No attendance snapshot is configured for {ChatRegion.DisplayName(region)}.");
}
var allowedOrigins = (builder.Configuration["F3_CHAT_ALLOWED_ORIGINS"] ??
    "http://localhost:5090,http://127.0.0.1:5090,http://localhost:5173,http://127.0.0.1:5173")
    .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
builder.Services.AddSingleton<Func<string, AnalyticsChatService>>(sp =>
{
    var http = new HttpClient { Timeout = TimeSpan.FromSeconds(100) };
    return region => new AnalyticsChatService(http, DatabaseFor(region), openRouterKey, defaultModel, allowedModels,
        sp.GetService<IChatTelemetry>(),
        ex => sp.GetRequiredService<ILogger<AnalyticsChatService>>().LogWarning("Chat telemetry was not saved: {ErrorType}", ex.GetType().Name),
        ChatRegion.Normalize(region));
});
builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy
            .WithOrigins(allowedOrigins)
            .AllowAnyHeader()
            .AllowAnyMethod();
    });
});

var app = builder.Build();
// The hosted Function URL already owns CORS; do not emit duplicate headers.
if (!hosted) app.UseCors();

if (s3 != null) app.Lifetime.ApplicationStopped.Register(s3.Dispose);
if (hosted) app.Logger.LogWarning("Saved-chat persistence is not configured for Lambda; chat bodies are not saved.");
var lambda = new Lazy<Function>(() => new Function());

// Shared local/Lambda chat routes; deployment sets the allowed frontend origin.
app.MapGet("/chat/status", async (string? region, CancellationToken ct) =>
{
    try
    {
        return Results.Ok(new { configured = openRouterKey.Length > 0 && defaultModel.Length > 0,
            model = defaultModel, models = allowedModels, region = ChatRegion.Normalize(region ?? "southfork"), snapshot = await DatabaseFor(region ?? "southfork").GetSnapshotAsync(ct) });
    }
    catch (Exception ex) when (ex is InvalidOperationException or ArgumentException or System.ComponentModel.Win32Exception)
    {
        return Results.Json(new { error = "The attendance snapshot is unavailable. Check backend setup." }, statusCode: 503);
    }
});

var chatGate = new SemaphoreSlim(2);
app.Use(async (context, next) =>
{
    if (context.Request.Path.StartsWithSegments("/chat") && context.Request.ContentLength > 64 * 1024)
    {
        context.Response.StatusCode = 413;
        return;
    }
    if (context.Request.Path.StartsWithSegments("/chat") &&
        context.Features.Get<Microsoft.AspNetCore.Http.Features.IHttpMaxRequestBodySizeFeature>() is { IsReadOnly: false } limit)
        limit.MaxRequestBodySize = 64 * 1024;
    await next(context);
});
app.MapPost("/chat", async (ChatRequest request, Func<string, AnalyticsChatService> chatFor, CancellationToken ct) =>
{
    if (!await chatGate.WaitAsync(0, ct))
        return Results.Json(new { error = "The assistant is busy. Please try again shortly." }, statusCode: 429);
    try { return Results.Ok(await chatFor(request.Region).ChatAsync(request, ct)); }
    catch (ChatQueryBudgetException ex) { return Results.Json(new { error = ex.Message, code = "query_attempt_limit", attempts = ex.Attempts }, statusCode: 502); }
    catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }
    catch (InvalidOperationException ex) { return Results.Json(new { error = ex.Message }, statusCode: 503); }
    catch (HttpRequestException ex) { return Results.Json(new { error = ex.Message }, statusCode: 502); }
    catch (OperationCanceledException) { return Results.Json(new { error = "That question took too long. Try a more specific question." }, statusCode: 504); }
    catch (System.ComponentModel.Win32Exception) { return Results.Json(new { error = "The query engine is unavailable. Check backend setup." }, statusCode: 503); }
    finally { chatGate.Release(); }
});

// NDJSON events over a POST response; the JSON endpoint remains available for evals.
app.MapPost("/chat/stream", async (ChatRequest request, Func<string, AnalyticsChatService> chatFor, HttpContext context, CancellationToken ct) =>
{
    if (!await chatGate.WaitAsync(0, ct)) { context.Response.StatusCode = 429; return; }
    context.Response.ContentType = "application/x-ndjson";
    context.Response.Headers.CacheControl = "no-cache, no-transform";
    context.Response.Headers["X-Accel-Buffering"] = "no";
    async Task Emit(object value)
    {
        await context.Response.WriteAsync(JsonSerializer.Serialize(value, new JsonSerializerOptions(JsonSerializerDefaults.Web)) + "\n", ct);
        await context.Response.Body.FlushAsync(ct);
    }
    try
    {
        var reply = await chatFor(request.Region).ChatAsync(request, ct, (type, text) => Emit(new { type, text }));
        await Emit(new { type = "complete", reply });
    }
    catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
    catch (Exception ex)
    {
        app.Logger.LogWarning("Chat stream failed: {ErrorType}", ex.GetType().Name);
        if (!ct.IsCancellationRequested)
            await Emit(new { type = "error", text = ex is OperationCanceledException
                ? "That took too long. Try a more specific question."
                : "I couldn't finish that question. Please try again." });
    }
    finally { chatGate.Release(); }
});

if (!hosted)
{
    var adminPassword = builder.Configuration["F3_CHAT_ADMIN_PASSWORD"];
    var admin = app.MapGroup("/admin/chats");
    admin.AddEndpointFilter(async (context, next) =>
    {
        var http = context.HttpContext;
        var origin = http.Request.Headers.Origin.ToString();
        var localOrigin = origin.Length == 0 || new[] { "http://localhost:5173", "http://127.0.0.1:5173",
            "http://localhost:5090", "http://127.0.0.1:5090" }.Contains(origin);
        if (http.Connection.RemoteIpAddress is not { } ip || !System.Net.IPAddress.IsLoopback(ip) ||
            http.Request.Host.Host is not ("localhost" or "127.0.0.1" or "[::1]" or "::1") || !localOrigin)
            return Results.StatusCode(403);
        http.Response.Headers.CacheControl = "no-store";
        if (string.IsNullOrEmpty(adminPassword)) return Results.StatusCode(503);
        var supplied = http.Request.Headers["X-Chat-Admin-Password"].ToString();
        if (!System.Security.Cryptography.CryptographicOperations.FixedTimeEquals(
            System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(supplied)),
            System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(adminPassword))))
            return Results.StatusCode(401);
        try { return await next(context); }
        catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }
        catch (Exception ex) when (ex is IOException or OperationCanceledException or System.ComponentModel.Win32Exception)
        { return Results.Json(new { error = "Chat logs are temporarily unavailable. Try refreshing." }, statusCode: 503); }
    });
    admin.MapGet("", async (DuckDbChatTelemetry logs, int? offset, string? search, string? status, CancellationToken ct) =>
    {
        var rows = await logs.ListAsync(offset ?? 0, search, status, ct);
        return Results.Ok(new { items = rows.Take(25), hasMore = rows.Length > 25 });
    });
    admin.MapGet("/{id:guid}", async (Guid id, DuckDbChatTelemetry logs, CancellationToken ct) =>
    {
        var row = await logs.GetAsync(id, ct);
        return row.HasValue ? Results.Ok(row.Value) : Results.NotFound();
    });

}
else
{
    app.MapGet("/admin/chats", () => Results.StatusCode(503));
    app.MapGet("/admin/chats/{id:guid}", () => Results.StatusCode(503));
}

app.MapGet("/health", () => Results.Ok(new { status = "ok" }));
app.MapGet("/", () => Results.Ok(new
{
    service = hosted ? "F3 Analytics API" : "F3 Lambda Local API",
    post = "/",
    skipMomento = CacheHelper.ShouldSkipMomento,
    regionConfigFile = Environment.GetEnvironmentVariable(S3RegionConfigProvider.FileEnvironmentVariable)
}));

app.MapPost("/", async (HttpRequest httpRequest) =>
{
    using var reader = new StreamReader(httpRequest.Body);
    var body = await reader.ReadToEndAsync();

    var request = new APIGatewayHttpApiV2ProxyRequest
    {
        Body = body
    };

    var result = await lambda.Value.FunctionHandler(request, context: null!);

    if (result is APIGatewayProxyResponse proxyResponse)
    {
        return Results.Text(proxyResponse.Body, "application/json", statusCode: proxyResponse.StatusCode);
    }

    if (result is string rawValue)
    {
        return Results.Text(rawValue, "application/json");
    }

    return Results.Text(JsonSerializer.Serialize(result), "application/json");
});

app.Run();

static void UseLambdaProjectDirectoryForLocalSecrets()
{
    var currentDirectory = Directory.GetCurrentDirectory();
    var candidateDirectories = new[]
    {
        Path.Combine(currentDirectory, "f3-data-tools-backend", "F3Lambda"),
        Path.Combine(currentDirectory, "F3Lambda"),
        Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "F3Lambda"))
    };

    var lambdaProjectDirectory = candidateDirectories.FirstOrDefault(Directory.Exists);
    if (lambdaProjectDirectory != null)
    {
        Directory.SetCurrentDirectory(lambdaProjectDirectory);
    }
}
