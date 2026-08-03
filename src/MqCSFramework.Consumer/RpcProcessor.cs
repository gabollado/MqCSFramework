using System.Text.Json;

namespace MqCSFramework;

/// <summary>
/// Abstract base class for RPC processors.
/// Handles deserialization and response serialization internally — the consumer calls ProcessRawRpcAsync directly (no reflection).
/// Inherit this in your processor implementation.
/// </summary>
public abstract class RpcProcessor<TRequest, TResponse> : IRpcProcessor<TRequest, TResponse>
    where TRequest : class
    where TResponse : class
{
    public async Task<byte[]> ProcessRawRpcAsync(ReadOnlyMemory<byte> body, MessageContext context, CancellationToken ct = default)
    {
        var request = JsonSerializer.Deserialize<TRequest>(body.Span);
        if (request is null)
        {
            throw new MessageSerializationException(
                $"Failed to deserialize RPC request to type '{typeof(TRequest).FullName}'.",
                context.MessageId);
        }

        var response = await ProcessAsync(request, context, ct);
        return JsonSerializer.SerializeToUtf8Bytes(response);
    }

    public abstract Task<TResponse> ProcessAsync(TRequest request, MessageContext context, CancellationToken ct = default);
}
