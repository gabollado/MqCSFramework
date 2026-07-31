namespace MqCSFramework;

/// <summary>
/// Per-message options for standard sends (override sender defaults).
/// </summary>
public sealed class SendOptions
{
    public string? RoutingKey { get; set; }
    public string? CorrelationId { get; set; }
    public IReadOnlyDictionary<string, string>? AdditionalHeaders { get; set; }
}
