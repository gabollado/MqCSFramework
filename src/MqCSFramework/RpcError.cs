namespace MqCSFramework;

/// <summary>
/// Describes a business failure returned by an RPC processor.
/// Both members are required together, so a partially-populated error state cannot exist.
/// </summary>
public sealed record RpcError
{
    /// <summary>
    /// Machine-readable, service-specific error code (e.g. "SKU_NOT_FOUND"). Not a framework enum.
    /// </summary>
    public required string Code { get; init; }

    /// <summary>
    /// Human-readable description for logs and diagnostics.
    /// </summary>
    public required string Message { get; init; }
}
