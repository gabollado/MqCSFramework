using System.Text.Json;
using System.Text.Json.Nodes;

namespace MqCSFramework.Abstractions.Internal;

/// <summary>
/// Masks the values of specified fields in a JSON string, replacing them with "***MASKED***".
/// The original message body is never modified; this utility is intended for logging only.
/// </summary>
internal static class MessageMasker
{
    private const string MaskValue = "***MASKED***";

    /// <summary>
    /// Returns a copy of <paramref name="json"/> where any field whose name (case-insensitive)
    /// appears in <paramref name="maskedFields"/> has its value replaced with "***MASKED***".
    /// Nested objects and arrays are traversed recursively.
    /// If <paramref name="maskedFields"/> is null or empty the original string is returned unchanged.
    /// If <paramref name="json"/> is null or whitespace, it is returned as-is.
    /// </summary>
    public static string Mask(string? json, HashSet<string>? maskedFields)
    {
        if (maskedFields is null || maskedFields.Count == 0)
            return json ?? string.Empty;

        if (string.IsNullOrWhiteSpace(json))
            return json ?? string.Empty;

        // Quick check: if none of the field names appear in the raw JSON string,
        // skip the full parse + rewrite. False positives (field name appears inside
        // a value) are harmless — they just fall through to the full rewrite.
        if (!ContainsAnyField(json, maskedFields))
            return json;

        JsonNode? root;
        try
        {
            root = JsonNode.Parse(json);
        }
        catch (JsonException)
        {
            // Invalid JSON — return as-is rather than throwing.
            return json;
        }

        if (root is null)
            return json;

        MaskNode(root, maskedFields);

        return root.ToJsonString(new JsonSerializerOptions { WriteIndented = false });
    }

    /// <summary>
    /// Creates a case-insensitive HashSet from the provided field names for efficient lookup.
    /// Returns null if the input is null or empty.
    /// </summary>
    public static HashSet<string>? BuildFieldSet(IList<string>? fieldNames)
    {
        if (fieldNames is null || fieldNames.Count == 0)
            return null;

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
                MaskArray(array, maskedFields);
                break;
        }
    }

    private static void MaskObject(JsonObject obj, HashSet<string> maskedFields)
    {
        // Collect property names first to avoid modifying the collection while iterating.
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
                MaskNode(child, maskedFields);
        }
    }

    private static void MaskArray(JsonArray array, HashSet<string> maskedFields)
    {
        foreach (var item in array)
        {
            if (item is not null)
                MaskNode(item, maskedFields);
        }
    }

    private static bool ContainsAnyField(string json, HashSet<string> maskedFields)
    {
        foreach (var field in maskedFields)
        {
            if (json.Contains(field, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }
}
