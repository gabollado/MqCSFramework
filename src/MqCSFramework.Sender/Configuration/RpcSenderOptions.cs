namespace MqCSFramework;

/// <summary>
/// Configuration for an RPC (request-reply) sender.
/// </summary>
public sealed class RpcSenderOptions
{
    public RabbitMqConnectionOptions Connection { get; set; } = new();
    public string Exchange { get; set; } = "";
    public string RoutingKey { get; set; } = "";

    /// <summary>
    /// How long to wait for an RPC response before timing out, in milliseconds. Default 30000 (30s).
    /// </summary>
    public int TimeoutMs { get; set; } = 30000;

    /// <summary>
    /// Number of additional attempts after the first on an exceptional failure
    /// (timeout, transport/connection error, or response deserialization failure).
    /// 0 (default) disables retry. A business failure (Success = false) is never retried.
    /// </summary>
    public int MaxExceptionRetries { get; set; } = 0;

    /// <summary>
    /// Delay waited before each retry attempt, in milliseconds. Default 1000 (1s).
    /// </summary>
    public int ExceptionRetryDelayMs { get; set; } = 1000;
}
