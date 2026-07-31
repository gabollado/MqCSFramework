namespace MqCSFramework;

/// <summary>
/// Thrown when an RPC call times out waiting for a response.
/// </summary>
public sealed class RpcTimeoutException : Exception
{
    public string CorrelationId { get; }
    public TimeSpan Timeout { get; }

    public RpcTimeoutException(string correlationId, TimeSpan timeout)
        : base($"RPC call {correlationId} timed out after {timeout.TotalSeconds}s")
    {
        CorrelationId = correlationId;
        Timeout = timeout;
    }
}
