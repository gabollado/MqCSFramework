namespace MqCSFramework;

/// <summary>
/// Thrown when the remote processor threw an exception during RPC processing.
/// </summary>
public sealed class RpcRemoteException : Exception
{
    public string CorrelationId { get; }
    public string RemoteExceptionType { get; }

    public RpcRemoteException(string correlationId, string message, string? remoteExceptionType = null)
        : base($"Remote processor error for {correlationId}: {message}")
    {
        CorrelationId = correlationId;
        RemoteExceptionType = remoteExceptionType ?? "Unknown";
    }
}
