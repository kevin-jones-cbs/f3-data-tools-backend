using Amazon.Lambda.APIGatewayEvents;
using F3Lambda;
using F3Lambda.Data;
using System.IO.Compression;
using System.Text.Json;

if (args.Length != 2)
    throw new ArgumentException("Usage: Export <region> <output.json> (run from F3Lambda directory)");
var region = args[0];
using var regions = JsonDocument.Parse(await File.ReadAllTextAsync("regions.json"));
if (!regions.RootElement.GetProperty("regions").EnumerateArray()
    .Any(r => r.GetProperty("queryStringValue").GetString() == region))
    throw new ArgumentException($"Unknown region: {region}");

Environment.SetEnvironmentVariable(CacheHelper.SkipMomentoEnvironmentVariable, "true");
Environment.SetEnvironmentVariable(S3RegionConfigProvider.DisableS3EnvironmentVariable, "true");
Environment.SetEnvironmentVariable(S3RegionConfigProvider.FileEnvironmentVariable,
    Path.GetFullPath("regions.json"));

// Invoke only the read action, reusing the application's region mapping and parsing.
var result = await new Function().FunctionHandler(new APIGatewayHttpApiV2ProxyRequest
{
    Body = JsonSerializer.Serialize(new { Action = "GetAllPosts", Region = region })
}, null!);
if (result is not string encoded)
    throw new InvalidOperationException($"{region} export failed; see backend diagnostics above.");

var bytes = Convert.FromBase64String(encoded);
// The backend prefixes its gzip stream with a four-byte uncompressed length.
using var stream = new MemoryStream(bytes, 4, bytes.Length - 4);
using var gzip = new GZipStream(stream, CompressionMode.Decompress);
using var reader = new StreamReader(gzip);
var json = await reader.ReadToEndAsync();
using var document = JsonDocument.Parse(json);
if (document.RootElement.GetProperty("posts").GetArrayLength() == 0)
    throw new InvalidOperationException("Refusing to export an empty attendance dataset.");
await File.WriteAllTextAsync(args[1], json);
Console.WriteLine($"Exported fresh {region} data (Momento bypassed).");
