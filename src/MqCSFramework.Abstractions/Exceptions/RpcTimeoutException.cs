namespace MqCSFramework.Abstractions.Exceptions;

/// <summary>
/// RPC call timed out waiting for response.
/// </summary>
public class RpcTimeoutException : MqException
{
    public string CorrelationId { get; }
    public string MessageId { get; }
    public TimeSpan Timeout { get; }

    public RpcTimeoutException(string correlationId, string messageId, TimeSpan timeout)
        : base($"RPC call timed out after {timeout.TotalSeconds}s. CorrelationId: {correlationId}, MessageId: {messageId}")
    {
        CorrelationId = correlationId;
        MessageId = messageId;
        Timeout = timeout;
    }
}
