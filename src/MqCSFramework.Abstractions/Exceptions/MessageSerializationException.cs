namespace MqCSFramework.Abstractions.Exceptions;

/// <summary>
/// Serialization/deserialization failure.
/// </summary>
public class MessageSerializationException : MqException
{
    public string? MessageId { get; }
    public Type? TargetType { get; }

    public MessageSerializationException(string message, string? messageId = null, Type? targetType = null, Exception? innerException = null)
        : base(message, innerException!)
    {
        MessageId = messageId;
        TargetType = targetType;
    }
}
