using System.Text;
using System.Text.Json.Nodes;

namespace F3Lambda.Analytics;

public static class OpenRouterStream
{
    // Reconstruct the normal response for tool execution and telemetry while
    // forwarding only public content (never reasoning or tool arguments).
    public static async Task<JsonObject> ReadAsync(Stream stream, Func<string, Task>? onText,
        ChatModelCall call, CancellationToken ct)
    {
        using var reader = new StreamReader(stream);
        var message = new JsonObject { ["role"] = "assistant" };
        var choice = new JsonObject { ["message"] = message };
        var result = new JsonObject { ["choices"] = new JsonArray(choice) };
        call.Output = result; // Retain partial output on cancellation/provider failure.
        var frame = new StringBuilder();
        var size = 0;
        while (await reader.ReadLineAsync(ct) is { } line)
        {
            size += line.Length;
            if (size > 2_000_000) throw new HttpRequestException("Model stream exceeded the response limit.");
            if (line.StartsWith("data:")) { frame.AppendLine(line[5..].TrimStart()); continue; }
            if (line.Length != 0 || frame.Length == 0) continue;
            var data = frame.ToString().Trim(); frame.Clear();
            if (data == "[DONE]")
            {
                if (choice["finish_reason"]?.GetValue<string>() is not ("stop" or "tool_calls"))
                    throw new HttpRequestException("Model stream did not complete its answer.");
                return result;
            }
            var chunk = JsonNode.Parse(data)!.AsObject();
            foreach (var key in new[] { "id", "model", "usage", "provider", "error" })
                if (chunk[key] != null) result[key] = chunk[key]!.DeepClone();
            if (chunk["error"] != null) throw new HttpRequestException("OpenRouter reported a streaming error.");
            if (chunk["choices"] is not JsonArray { Count: > 0 } choices) continue;
            var first = choices[0]!;
            if (first["finish_reason"] != null) choice["finish_reason"] = first["finish_reason"]!.DeepClone();
            if (first["delta"] is not JsonObject delta) continue;
            Merge(message, delta);
            if (delta["content"] is JsonValue content && !string.IsNullOrWhiteSpace(content.GetValue<string>()) &&
                message["tool_calls"] == null && onText != null)
                await onText(message["content"]?.GetValue<string>() ?? "");
        }
        throw new HttpRequestException("Model stream ended before completion.");
    }

    private static void Merge(JsonObject target, JsonObject delta)
    {
        foreach (var (key, value) in delta)
        {
            if (value == null || key == "index") continue;
            if (value is JsonObject obj)
            {
                if (target[key] is not JsonObject) target[key] = new JsonObject();
                Merge(target[key]!.AsObject(), obj);
            }
            else if (value is JsonArray array)
            {
                if (target[key] is not JsonArray) target[key] = new JsonArray();
                var dest = target[key]!.AsArray();
                foreach (var item in array)
                {
                    var index = item?["index"]?.GetValue<int>() ?? dest.Count;
                    if (index < 0 || index > 100) throw new HttpRequestException("Invalid stream fragment index.");
                    while (dest.Count <= index) dest.Add(new JsonObject());
                    Merge(dest[index]!.AsObject(), item!.AsObject());
                }
            }
            else if (key is "content" or "arguments" or "reasoning" or "text" or "summary" or "data")
                target[key] = (target[key]?.GetValue<string>() ?? "") + value.GetValue<string>();
            else target[key] = value.DeepClone();
        }
    }
}
