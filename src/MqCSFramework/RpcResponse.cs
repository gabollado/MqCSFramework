using System.Text.Json.Serialization;

namespace MqCSFramework;

/// <summary>
/// Abstract base record that every RPC response type must extend.
/// Carries the operation outcome; the concrete response record adds its own payload members.
/// </summary>
/// <remarks>
/// A processor signals a business failure purely by setting <see cref="Error"/> on the response
/// it returns — it does not throw. <see cref="Success"/> is derived from <see cref="Error"/> so the
/// two can never disagree, and is not serialized (the receiver recomputes it after deserialization).
/// </remarks>
public abstract record RpcResponse
{
    /// <summary>
    /// The business failure, or null on success.
    /// </summary>
    public RpcError? Error { get; init; }

    /// <summary>
    /// True when there is no <see cref="Error"/>. Computed, not stored, and not written to the wire.
    /// </summary>
    [JsonIgnore]
    public bool Success => Error is null;
}
