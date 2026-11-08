namespace MqCSFramework;

/// <summary>
/// Configuration for a standard (fire-and-forget) sender.
/// </summary>
public sealed class StandardSenderOptions
{
    public RabbitMqConnectionOptions Connection { get; set; } = new();
    public string Exchange { get; set; } = "";
    public string RoutingKey { get; set; } = "";

    /// <summary>
    /// Upper bound for one send (connecting if needed, then publishing), in milliseconds. Default 20000 (20s).
    /// Applied together with the caller's cancellation token; when it expires the send fails with
    /// <see cref="TimeoutException"/>. 0 or less disables it.
    /// </summary>
    public int TimeoutMs { get; set; } = 20000;
}
