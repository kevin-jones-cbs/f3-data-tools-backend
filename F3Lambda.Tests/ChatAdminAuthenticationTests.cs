using F3Lambda.Analytics;
using Xunit;

namespace F3Lambda.Tests;

public class ChatAdminAuthenticationTests
{
    [Fact]
    public void RequiresConfiguredMatchingPassword()
    {
        Assert.Equal(503, new ChatAdminAuthentication(null).Check("anything"));
        Assert.Equal(503, new ChatAdminAuthentication("").Check(""));
        var auth = new ChatAdminAuthentication("test-password");
        Assert.Equal(401, auth.Check(""));
        Assert.Equal(401, auth.Check("wrong"));
        Assert.Equal(401, auth.Check(new string('x', 257)));
        Assert.Equal(200, auth.Check("test-password"));
    }

    [Fact]
    public void ThrottlesFailedAttemptsAndRecoversAfterWindow()
    {
        var clock = new Clock();
        var auth = new ChatAdminAuthentication("test-password", clock);
        for (int i = 0; i < 10; i++) Assert.Equal(401, auth.Check("wrong"));
        Assert.Equal(429, auth.Check("wrong"));
        Assert.Equal(429, auth.Check("test-password"));
        clock.Now = clock.Now.AddMinutes(1);
        Assert.Equal(200, auth.Check("test-password"));
    }

    private sealed class Clock : TimeProvider
    {
        public DateTimeOffset Now = new(2026, 10, 4, 0, 0, 0, TimeSpan.Zero);
        public override DateTimeOffset GetUtcNow() => Now;
    }
}
