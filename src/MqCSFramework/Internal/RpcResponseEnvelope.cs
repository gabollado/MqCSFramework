namespace MqCSFramework.Internal;

/// <summary>
/// Wire format for RPC responses. Wraps either a success payload or an error.
/// </summary>
internal sealed record RpcResponseEnvelope
{
    public required bool IsError { get; init; }
    public byte[]? Payload { get; init; }
    public string? ErrorMessage { get; init; }
    public string? ErrorType { get; init; }
}
