namespace MqCSFramework;

/// <summary>
/// Non-generic base interface for RPC processors.
/// The consumer calls ProcessRawRpcAsync — the implementation deserializes, processes, and serializes the response.
/// </summary>
public interface IRpcProcessor
{
    Task<byte[]> ProcessRawRpcAsync(ReadOnlyMemory<byte> body, MessageContext context, CancellationToken ct = default);
}

/// <summary>
/// Generic interface for RPC processors that return a typed response.
/// Define a contract interface inheriting this in your shared contracts package.
/// </summary>
public interface IRpcProcessor<in TRequest, TResponse> : IRpcProcessor where TRequest : class where TResponse : class
{
    Task<TResponse> ProcessAsync(TRequest request, MessageContext context, CancellationToken ct = default);
}
