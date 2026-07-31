namespace MqCSFramework.Abstractions.Models;

/// <summary>
/// The internal envelope that wraps a serialized message for transport.
/// </summary>
public sealed record MessageEnvelope
{
    public required byte[] Body { get; init; }
    public required string MessageId { get; init; }
    public required string MessageType { get; init; }
    public string? CorrelationId { get; init; }
    public string? ReplyTo { get; init; }
    public string? Exchange { get; init; }
    public string? RoutingKey { get; init; }
    public string ContentType { get; init; } = "application/json";
    public bool Persistent { get; init; } = true;
    public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.UtcNow;
    public string? SenderIdentity { get; init; }
    public IDictionary<string, object?> Headers { get; init; } = new Dictionary<string, object?>();
}
