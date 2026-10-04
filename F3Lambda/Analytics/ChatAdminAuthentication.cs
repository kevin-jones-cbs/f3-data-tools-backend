using System.Security.Cryptography;
using System.Text;

namespace F3Lambda.Analytics;

// Fail closed when unconfigured. Failed attempts are bounded per running instance.
public sealed class ChatAdminAuthentication(string? password, TimeProvider? clock = null)
{
    private readonly byte[]? expected = string.IsNullOrEmpty(password) ? null : SHA256.HashData(Encoding.UTF8.GetBytes(password));
    private readonly TimeProvider time = clock ?? TimeProvider.System;
    private readonly object gate = new();
    private DateTimeOffset windowStart;
    private int failures;

    public int Check(string supplied)
    {
        if (expected == null) return 503;
        lock (gate)
        {
            var now = time.GetUtcNow();
            if (now - windowStart >= TimeSpan.FromMinutes(1)) { windowStart = now; failures = 0; }
            if (failures >= 10) return 429;
            if (supplied.Length > 0 && supplied.Length <= 256 && CryptographicOperations.FixedTimeEquals(
                SHA256.HashData(Encoding.UTF8.GetBytes(supplied)), expected)) return 200;
            failures++;
            return 401;
        }
    }
}
