namespace MqCSFramework;

/// <summary>
/// Thrown when message serialization or deserialization fails.
/// </summary>
public sealed class MessageSerializationException : Exception
{
    public string? MessageId { get; }

    public MessageSerializationException(string message, string? messageId = null, Exception? inner = null)
        : base(message, inner)
    {
        MessageId = messageId;
    }
}
