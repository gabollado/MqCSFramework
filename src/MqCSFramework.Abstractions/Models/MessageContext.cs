namespace MqCSFramework.Abstractions.Models;

/// <summary>
/// Context passed to processors. Contains metadata about the received message.
/// </summary>
public sealed record MessageContext
{
    public required string MessageId { get; init; }
    public required string CorrelationId { get; init; }
    public required string MessageType { get; init; }
    public DateTimeOffset Timestamp { get; init; }
    public string? SenderIdentity { get; init; }
    public IReadOnlyDictionary<string, object?> Headers { get; init; } = new Dictionary<string, object?>();
    public bool Redelivered { get; init; }
    public CancellationToken CancellationToken { get; init; }
}
