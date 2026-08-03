using System.Text.Json;
using System.Text.Json.Nodes;

namespace MqCSFramework.Consumer.Internal;

/// <summary>
/// Masks sensitive field values in JSON strings for logging purposes.
/// Replaces values of matching fields with "***MASKED***" (case-insensitive match).
/// </summary>
internal static class LogMaskingHelper
{
    private const string MaskValue = "***MASKED***";

    /// <summary>
    /// Returns a copy of the JSON string with masked field values.
    /// Returns the original string unchanged if maskedFields is null/empty or json is invalid.
    /// </summary>
    public static string Mask(string? json, HashSet<string>? maskedFields)
    {
        if (maskedFields is null || maskedFields.Count == 0)
        {
            return json ?? string.Empty;
        }

        if (string.IsNullOrWhiteSpace(json))
        {
            return json ?? string.Empty;
        }

        JsonNode? root;
        try
        {
            root = JsonNode.Parse(json);
        }
        catch (JsonException)
        {
            return json;
        }

        if (root is null)
        {
            return json;
        }

        MaskNode(root, maskedFields);
        return root.ToJsonString(new JsonSerializerOptions { WriteIndented = false });
    }

    /// <summary>
    /// Creates a case-insensitive HashSet from field names for efficient lookup.
    /// Returns null if the input is null or empty.
    /// </summary>
    public static HashSet<string>? BuildFieldSet(IReadOnlyList<string>? fieldNames)
    {
        if (fieldNames is null || fieldNames.Count == 0)
        {
            return null;
        }

        return new HashSet<string>(fieldNames, StringComparer.OrdinalIgnoreCase);
    }

    private static void MaskNode(JsonNode node, HashSet<string> maskedFields)
    {
        switch (node)
        {
            case JsonObject obj:
                MaskObject(obj, maskedFields);
                break;
            case JsonArray array:
                foreach (var item in array)
                {
                    if (item is not null)
                    {
                        MaskNode(item, maskedFields);
                    }
                }
                break;
        }
    }

    private static void MaskObject(JsonObject obj, HashSet<string> maskedFields)
    {
        var propertyNames = obj.Select(p => p.Key).ToList();

        foreach (var name in propertyNames)
        {
            if (maskedFields.Contains(name))
            {
                obj[name] = MaskValue;
                continue;
            }

            var child = obj[name];
            if (child is not null)
            {
                MaskNode(child, maskedFields);
            }
        }
    }
}
