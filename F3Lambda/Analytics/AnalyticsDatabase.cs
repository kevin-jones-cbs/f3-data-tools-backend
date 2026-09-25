using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace F3Lambda.Analytics;

// An S3 implementation can download a versioned object into /tmp and return its
// local path here. Query execution and chat orchestration need not change.
public interface IAnalyticsSnapshotProvider
{
    Task<string> GetLocalPathAsync(CancellationToken cancellationToken);
}

public sealed class LocalAnalyticsSnapshotProvider(string path) : IAnalyticsSnapshotProvider
{
    public Task<string> GetLocalPathAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(path)) throw new InvalidOperationException("Analytics database missing. Run tools/southfork-duckdb/refresh.py first.");
        return Task.FromResult(Path.GetFullPath(path));
    }
}

public record QueryResult(string Sql, string[] Columns, JsonElement[][] Rows, bool Truncated);
public record SnapshotInfo(string RefreshedAt, string FirstDate, string LastDate, string Sha256);

public interface IAnalyticsDatabase
{
    Task<QueryResult> QueryAsync(string sql, CancellationToken cancellationToken);
    Task<SnapshotInfo> GetSnapshotAsync(CancellationToken cancellationToken);
}

public sealed class DuckDbAnalyticsDatabase(IAnalyticsSnapshotProvider snapshots, string executable = "duckdb", string? expectedRegion = null) : IAnalyticsDatabase
{
    public const int RowLimit = 100;
    public static string ValidateSql(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql) || sql.Length > 16000)
            throw new ArgumentException("Supply one SELECT query of at most 16,000 characters.");
        sql = sql.Trim().TrimEnd(';').TrimEnd();
        // Ignore SQL strings, quoted identifiers and comments when inspecting keywords.
        var tokens = Regex.Replace(sql, @"'(?:''|[^'])*'|""(?:""""|[^""])*""|--[^\r\n]*|/\*[\s\S]*?\*/", " ");
        if (!Regex.IsMatch(tokens.TrimStart(), @"\A(SELECT|WITH)\b", RegexOptions.IgnoreCase) ||
            tokens.Contains(';') || Regex.IsMatch(tokens,
                @"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|COPY|EXPORT|IMPORT|ATTACH|DETACH|INSTALL|LOAD|CALL|PRAGMA|SET|RESET|VACUUM|TRUNCATE|GETENV|QUERY|QUERY_TABLE)\b",
                RegexOptions.IgnoreCase))
            throw new ArgumentException("Only a single read-only SELECT (including WITH) is allowed.");
        return sql;
    }

    public async Task<QueryResult> QueryAsync(string sql, CancellationToken cancellationToken)
    {
        sql = ValidateSql(sql);
        var path = await snapshots.GetLocalPathAsync(cancellationToken);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));
        var start = new ProcessStartInfo(executable)
        {
            RedirectStandardOutput = true, RedirectStandardError = true,
            RedirectStandardInput = true, UseShellExecute = false, CreateNoWindow = true
        };
        // The subprocess cannot inherit the OpenRouter key or Google credentials.
        var searchPath = Environment.GetEnvironmentVariable("PATH");
        start.Environment.Clear();
        if (searchPath != null) start.Environment["PATH"] = searchPath;
        foreach (var arg in new[] { "-readonly", "-no-init", "-batch", "-bail", "-json", "-cmd",
            "SET temp_directory=''; SET memory_limit='256MB'; SET threads=2; SET autoinstall_known_extensions=false; SET autoload_known_extensions=false; SET enable_external_access=false; SET lock_configuration=true;",
            path, "-c", $"SELECT * FROM (\n{sql}\n) AS chat_result LIMIT {RowLimit + 1}" })
            start.ArgumentList.Add(arg);
        using var process = new Process { StartInfo = start };
        try
        {
            process.Start();
            process.StandardInput.Close();
            using var registration = timeout.Token.Register(() =>
            {
                try { if (!process.HasExited) process.Kill(entireProcessTree: true); } catch (InvalidOperationException) { }
            });
            var outputTask = ReadBoundedAsync(process.StandardOutput, 256_000, timeout.Token);
            var errorTask = ReadBoundedAsync(process.StandardError, 16_000, timeout.Token);
            await Task.WhenAll(outputTask, errorTask);
            await process.WaitForExitAsync(timeout.Token);
            if (process.ExitCode != 0)
                throw new ArgumentException("Query failed: " + (await errorTask).Replace(path, "[snapshot]"));
            var output = await outputTask;
            using var json = JsonDocument.Parse(string.IsNullOrWhiteSpace(output) ? "[]" : output);
            var objects = json.RootElement.EnumerateArray().ToArray();
            var columns = objects.Length == 0 ? [] : objects[0].EnumerateObject().Select(p => p.Name).ToArray();
            var rows = objects.Take(RowLimit).Select(row => columns.Select(column => row.GetProperty(column).Clone()).ToArray()).ToArray();
            return new QueryResult(sql, columns, rows, objects.Length > RowLimit);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new ArgumentException("Query exceeded the 10-second execution limit. Simplify the query.");
        }
        finally
        {
            try { if (!process.HasExited) process.Kill(entireProcessTree: true); } catch (InvalidOperationException) { }
        }
    }

    private static async Task<string> ReadBoundedAsync(StreamReader reader, int maximum, CancellationToken ct)
    {
        var text = new StringBuilder();
        var buffer = new char[4096];
        int count;
        while ((count = await reader.ReadAsync(buffer.AsMemory(), ct)) != 0)
        {
            if (text.Length + count > maximum) throw new ArgumentException("Query output is too large. Return a smaller aggregate result.");
            text.Append(buffer, 0, count);
        }
        return text.ToString();
    }

    public async Task<SnapshotInfo> GetSnapshotAsync(CancellationToken cancellationToken)
    {
        if (expectedRegion != null)
        {
            var regions = await QueryAsync("SELECT DISTINCT region FROM (SELECT region FROM posts UNION ALL SELECT region FROM qsource_posts UNION ALL SELECT region FROM pax UNION ALL SELECT region FROM aos UNION ALL SELECT region FROM historical_totals UNION ALL SELECT region FROM import_metadata)", cancellationToken);
            if (regions.Rows.Length != 1 || regions.Rows[0][0].GetString() != expectedRegion)
                throw new InvalidOperationException("The configured snapshot contains data for a different region.");
        }
        var result = await QueryAsync("SELECT CAST((SELECT refreshed_at FROM import_metadata LIMIT 1) AS VARCHAR) AS refreshed_at, CAST(min(date) AS VARCHAR) AS first_date, CAST(max(date) AS VARCHAR) AS last_date FROM posts", cancellationToken);
        var path = await snapshots.GetLocalPathAsync(cancellationToken);
        await using var file = File.OpenRead(path);
        var hash = Convert.ToHexString(await SHA256.HashDataAsync(file, cancellationToken)).ToLowerInvariant();
        return new SnapshotInfo(result.Rows[0][0].GetString()!, result.Rows[0][1].GetString()!, result.Rows[0][2].GetString()!, hash);
    }
}
