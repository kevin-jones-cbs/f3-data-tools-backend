using System.Text.RegularExpressions;

namespace F3Lambda.Analytics;

public static class ChatAnswerFormatting
{
    // Some models repeat tool results as plain-text tables despite instructions.
    // Only use this when the response already has a real results visualization.
    public static string WithoutTextTables(string answer)
    {
        var lines = answer.Replace("\r\n", "\n").Split('\n');
        var kept = new List<string>();
        for (var i = 0; i < lines.Length;)
        {
            var end = i;
            while (end < lines.Length && IsTableLine(lines[end])) end++;
            if (end - i >= 2)
            {
                // Remove an enclosing code fence as well, if it contains only this table.
                if (kept.Count > 0 && kept[^1].TrimStart().StartsWith("```") &&
                    end < lines.Length && lines[end].Trim() == "```")
                {
                    kept.RemoveAt(kept.Count - 1);
                    end++;
                }
                kept.Add("");
                i = end;
            }
            else kept.Add(lines[i++]);
        }
        var prose = Regex.Replace(string.Join("\n", kept), @"\n(?:[ \t]*\n){2,}", "\n\n").Trim();
        return prose.Length == 0 ? "Here are the results." : prose;
    }

    private static bool IsTableLine(string line) => line.Contains('|') &&
        line.Split('|').Count(cell => !string.IsNullOrWhiteSpace(cell)) >= 2;
}
