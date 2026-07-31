namespace MqCSFramework;

/// <summary>
/// Metadata available to processors when handling a message.
/// </summary>
public sealed record MessageContext
{
    public required string MessageId { get; init; }
    public required string CorrelationId { get; init; }
    public required DateTimeOffset Timestamp { get; init; }
    public required string Pattern { get; init; }
    public required IReadOnlyDictionary<string, string> Headers { get; init; }
}
