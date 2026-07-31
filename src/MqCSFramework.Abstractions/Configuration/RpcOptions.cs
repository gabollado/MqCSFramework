namespace MqCSFramework.Abstractions.Configuration;

/// <summary>
/// Options for sending an RPC request.
/// </summary>
public record RpcOptions
{
    public string? RoutingKey { get; init; }
    public string? CorrelationId { get; init; }
    public string? SenderIdentity { get; init; }
    public TimeSpan Timeout { get; init; } = TimeSpan.FromSeconds(30);
    public IDictionary<string, object?>? Headers { get; init; }
}
