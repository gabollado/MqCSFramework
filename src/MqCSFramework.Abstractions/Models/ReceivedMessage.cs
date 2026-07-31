namespace MqCSFramework.Abstractions.Models;

/// <summary>
/// A message received from the transport, before deserialization.
/// </summary>
public sealed record ReceivedMessage
{
    public required byte[] Body { get; init; }
    public required ulong DeliveryTag { get; init; }
    public required string MessageId { get; init; }
    public required string MessageType { get; init; }
    public string? CorrelationId { get; init; }
    public string? ReplyTo { get; init; }
    public string? Exchange { get; init; }
    public string? RoutingKey { get; init; }
    public string? ContentType { get; init; }
    public DateTimeOffset Timestamp { get; init; }
    public string? SenderIdentity { get; init; }
    public IReadOnlyDictionary<string, object?> Headers { get; init; } = new Dictionary<string, object?>();
    public bool Redelivered { get; init; }
}
