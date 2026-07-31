namespace MqCSFramework.Abstractions.Models;

/// <summary>
/// Standard error response for RPC failures. Serialized and sent back to caller.
/// </summary>
public sealed record RpcErrorResponse
{
    public bool IsError { get; init; } = true;
    public required string ErrorCode { get; init; }
    public required string ErrorMessage { get; init; }
    public string? StackTrace { get; init; }
}
