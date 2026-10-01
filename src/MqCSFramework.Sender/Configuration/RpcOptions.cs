namespace MqCSFramework;

/// <summary>
/// Per-message options for RPC sends (override sender defaults).
/// </summary>
public sealed class RpcOptions
{
    public string? RoutingKey { get; set; }

    /// <summary>
    /// Overrides the sender's <c>TimeoutMs</c> for this call (milliseconds).
    /// </summary>
    public int? TimeoutMs { get; set; }

    /// <summary>
    /// Overrides the sender's <c>MaxExceptionRetries</c> for this call.
    /// </summary>
    public int? MaxExceptionRetries { get; set; }

    /// <summary>
    /// Overrides the sender's <c>ExceptionRetryDelayMs</c> for this call (milliseconds).
    /// </summary>
    public int? ExceptionRetryDelayMs { get; set; }

    public IReadOnlyDictionary<string, string>? AdditionalHeaders { get; set; }
}
