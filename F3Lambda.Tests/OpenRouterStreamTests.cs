using System.Text;
using F3Lambda.Analytics;
using Xunit;

namespace F3Lambda.Tests;

public class OpenRouterStreamTests
{
    [Fact]
    public async Task StreamsTextAndRetainsUsageWhileIgnoringComments()
    {
        var chunks = new List<string>();
        var call = new ChatModelCall();
        using var stream = Data(": keepalive\n\n" +
            "data: {\"choices\":[{\"delta\":{\"content\":\"\"}}]}\n\n" +
            "data: {\"id\":\"gen-1\",\"choices\":[{\"delta\":{\"content\":\"Hello\",\"reasoning\":\"private\"}}]}\n\n" +
            "data: {\"choices\":[{\"delta\":{\"content\":\" world\"},\"finish_reason\":\"stop\"}]}\n\n" +
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":2}}\n\n" +
            "data: [DONE]\n\n");
        var result = await OpenRouterStream.ReadAsync(stream, text => { chunks.Add(text); return Task.CompletedTask; }, call, CancellationToken.None);
        Assert.Equal(new[] { "Hello", "Hello world" }, chunks);
        Assert.Equal(12, result["usage"]!["prompt_tokens"]!.GetValue<int>());
        Assert.Same(result, call.Output);
    }

    [Fact]
    public async Task ReassemblesToolArgumentsWithoutDisplayingThem()
    {
        using var stream = Data("""
            data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"q1","type":"function","function":{"name":"query_analytics","arguments":"{\"sql\":\"SELECT"}}]}}]}

            data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":" 1\"}"}}]},"finish_reason":"tool_calls"}]}

            data: [DONE]


            """);
        var result = await OpenRouterStream.ReadAsync(stream, _ => throw new Exception("Tool leaked to UI"), new(), CancellationToken.None);
        Assert.Equal("{\"sql\":\"SELECT 1\"}", result["choices"]![0]!["message"]!["tool_calls"]![0]!["function"]!["arguments"]!.GetValue<string>());
    }

    [Theory]
    [InlineData("data: {\"error\":{\"message\":\"failed\"}}\n\n")]
    [InlineData("data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")]
    [InlineData("data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"length\"}]}\n\ndata: [DONE]\n\n")]
    public async Task FailsOnProviderErrorTruncatedAnswerOrDroppedConnection(string data)
    {
        using var stream = Data(data);
        await Assert.ThrowsAsync<HttpRequestException>(() => OpenRouterStream.ReadAsync(stream, null, new(), CancellationToken.None));
    }
    private static MemoryStream Data(string data) => new(Encoding.UTF8.GetBytes(data));
}
