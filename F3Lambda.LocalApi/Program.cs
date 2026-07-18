using Amazon.Lambda.APIGatewayEvents;
using F3Lambda;
using F3Lambda.Data;
using System.Text.Json;

UseLambdaProjectDirectoryForLocalSecrets();

if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(CacheHelper.SkipMomentoEnvironmentVariable)))
{
    Environment.SetEnvironmentVariable(CacheHelper.SkipMomentoEnvironmentVariable, "true");
}

if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(S3RegionConfigProvider.FileEnvironmentVariable)) &&
    File.Exists("regions.json"))
{
    Environment.SetEnvironmentVariable(S3RegionConfigProvider.FileEnvironmentVariable, "regions.json");
}

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy
            .AllowAnyOrigin()
            .AllowAnyHeader()
            .AllowAnyMethod();
    });
});

var app = builder.Build();
app.UseCors();

var lambda = new Function();

app.MapGet("/", () => Results.Ok(new
{
    service = "F3 Lambda Local API",
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

    var result = await lambda.FunctionHandler(request, context: null!);

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
