namespace MqCSFramework;

/// <summary>
/// Per-message options for RPC sends (override sender defaults).
/// </summary>
public sealed class RpcOptions
{
    public string? RoutingKey { get; set; }
    public TimeSpan? Timeout { get; set; }
    public IReadOnlyDictionary<string, string>? AdditionalHeaders { get; set; }
}
