namespace MqCSFramework.Abstractions.Models;

/// <summary>
/// Result of processing a message, used to determine ACK/NACK behavior.
/// </summary>
public enum ProcessResult
{
    Success,
    Failure,
    Requeue
}
