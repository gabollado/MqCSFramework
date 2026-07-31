namespace MqCSFramework.Abstractions.Exceptions;

/// <summary>
/// RPC remote processor returned an error response.
/// </summary>
public class RpcRemoteException : MqException
{
    public string ErrorCode { get; }
    public string RemoteMessage { get; }
    public string? RemoteStackTrace { get; }

    public RpcRemoteException(string errorCode, string remoteMessage, string? remoteStackTrace = null)
        : base($"RPC remote error [{errorCode}]: {remoteMessage}")
    {
        ErrorCode = errorCode;
        RemoteMessage = remoteMessage;
        RemoteStackTrace = remoteStackTrace;
    }
}
