namespace MqCSFramework;

/// <summary>
/// Configuration for a standard (fire-and-forget) sender.
/// </summary>
public sealed class StandardSenderOptions
{
    public required RabbitMqConnectionOptions Connection { get; set; }
    public required string Exchange { get; set; }
    public string RoutingKey { get; set; } = "";
}
