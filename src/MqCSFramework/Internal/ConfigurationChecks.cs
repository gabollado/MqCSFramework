using System.Collections;
using System.Reflection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace MqCSFramework.Internal;

/// <summary>
/// Unknown configuration key detection shared by the Sender and Consumer modules.
/// </summary>
internal static class ConfigurationChecks
{
    /// <summary>
    /// Full configuration paths of the keys under <paramref name="section"/> that the configuration binder would not
    /// bind to <paramref name="optionsType"/>, checked recursively into nested settings objects such as Connection.
    /// </summary>
    public static IReadOnlyList<string> FindUnknownKeys(IConfigurationSection section, Type optionsType)
    {
        var unknownKeys = new List<string>();
        CollectUnknownKeys(section, optionsType, unknownKeys);
        return unknownKeys;
    }

    /// <summary>
    /// Logs each unknown key as a warning. The keys are still ignored, so a key disabled on purpose
    /// (or meant for a newer version) does not stop the application.
    /// </summary>
    public static void LogUnknownKeys(ILogger logger, IReadOnlyList<string> unknownKeys)
    {
        foreach (var key in unknownKeys)
        {
            logger.LogWarning(
                "Configuration key '{Key}' is not a known MqCSFramework setting and is ignored. It may be misspelled, or a setting that was renamed or removed.",
                key);
        }
    }

    private static void CollectUnknownKeys(IConfigurationSection section, Type optionsType, List<string> unknownKeys)
    {
        // The binder matches a key to a public property with a public setter, ignoring case.
        var properties = optionsType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.SetMethod is { IsPublic: true })
            .ToDictionary(p => p.Name, StringComparer.OrdinalIgnoreCase);

        foreach (var child in section.GetChildren())
        {
            if (!properties.TryGetValue(child.Key, out var property))
            {
                unknownKeys.Add(child.Path);
                continue;
            }

            if (IsSettingsObject(property.PropertyType))
            {
                CollectUnknownKeys(child, property.PropertyType, unknownKeys);
            }
        }
    }

    // Nested settings objects (e.g. Connection) have named keys to check; scalars and collection items don't.
    private static bool IsSettingsObject(Type type) =>
        type.IsClass && type != typeof(string) && !typeof(IEnumerable).IsAssignableFrom(type);
}
