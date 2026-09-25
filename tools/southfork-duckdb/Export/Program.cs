using Amazon.Lambda.APIGatewayEvents;
using F3Lambda;
using F3Lambda.Data;
using System.IO.Compression;
using System.Text.Json;

if (args.Length != 1)
    throw new ArgumentException("Usage: Export <output.json> (run from F3Lambda directory)");

Environment.SetEnvironmentVariable(CacheHelper.SkipMomentoEnvironmentVariable, "true");
Environment.SetEnvironmentVariable(S3RegionConfigProvider.DisableS3EnvironmentVariable, "true");
Environment.SetEnvironmentVariable(S3RegionConfigProvider.FileEnvironmentVariable,
    Path.GetFullPath("regions.json"));

// Invoke only the read action, reusing the application's region mapping and parsing.
var result = await new Function().FunctionHandler(new APIGatewayHttpApiV2ProxyRequest
{
    Body = """{"Action":"GetAllPosts","Region":"southfork"}"""
}, null!);
if (result is not string encoded)
    throw new InvalidOperationException("South Fork export failed; see backend diagnostics above.");

var bytes = Convert.FromBase64String(encoded);
// The backend prefixes its gzip stream with a four-byte uncompressed length.
using var stream = new MemoryStream(bytes, 4, bytes.Length - 4);
using var gzip = new GZipStream(stream, CompressionMode.Decompress);
using var reader = new StreamReader(gzip);
var json = await reader.ReadToEndAsync();
using var document = JsonDocument.Parse(json);
if (document.RootElement.GetProperty("posts").GetArrayLength() == 0)
    throw new InvalidOperationException("Refusing to export an empty attendance dataset.");
await File.WriteAllTextAsync(args[0], json);
Console.WriteLine("Exported fresh South Fork data (Momento bypassed).");
