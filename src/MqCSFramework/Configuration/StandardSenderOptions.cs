namespace MqCSFramework;

/// <summary>
/// Configuration for a standard (fire-and-forget) sender.
/// </summary>
public sealed class StandardSenderOptions
{
    public RabbitMqConnectionOptions Connection { get; set; } = new();
    public string Exchange { get; set; } = "";
    public string RoutingKey { get; set; } = "";
}
