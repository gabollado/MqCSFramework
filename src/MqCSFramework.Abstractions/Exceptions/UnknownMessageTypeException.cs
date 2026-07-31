namespace MqCSFramework.Abstractions.Exceptions;

/// <summary>
/// No processor registered for message type.
/// </summary>
public class UnknownMessageTypeException : MqException
{
    public string MessageType { get; }

    public UnknownMessageTypeException(string messageType)
        : base($"No processor registered for message type: {messageType}")
    {
        MessageType = messageType;
    }
}
