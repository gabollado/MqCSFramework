namespace MqCSFramework.Abstractions.Configuration;

/// <summary>
/// Options for sending a standard message.
/// </summary>
public record SendOptions
{
    public string? Exchange { get; init; }
    public string? RoutingKey { get; init; }
    public string? CorrelationId { get; init; }
    public string? SenderIdentity { get; init; }
    public bool Persistent { get; init; } = true;
    public IDictionary<string, object?>? Headers { get; init; }
}
