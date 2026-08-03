namespace MqCSFramework;

/// <summary>
/// Configuration for an RPC (request-reply) sender.
/// </summary>
public sealed class RpcSenderOptions
{
    public RabbitMqConnectionOptions Connection { get; set; } = new();
    public string Exchange { get; set; } = "";
    public string RoutingKey { get; set; } = "";
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(30);
}
