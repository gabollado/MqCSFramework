using MqCSFramework.Abstractions.Configuration;

namespace MqCSFramework.Abstractions.Sender;

/// <summary>
/// RPC sender. Publishes a request and awaits a typed response.
/// </summary>
public interface IRpcSender
{
    /// <summary>
    /// Send an RPC request specifying the target processor type.
    /// TRequest and TResponse are inferred from the processor's generic interface definition.
    /// TProcessor can be the concrete processor class or a shared contract interface.
    /// </summary>
    Task<TResponse> SendAsync<TProcessor, TResponse>(
        object request,
        RpcOptions? options = null,
        CancellationToken ct = default)
        where TProcessor : class
        where TResponse : class;

    /// <summary>
    /// Send an RPC request without specifying a processor (routes by message type).
    /// Both TRequest and TResponse must be specified explicitly.
    /// </summary>
    Task<TResponse> SendAsync<TRequest, TResponse>(
        TRequest request,
        RpcOptions? options = null,
        CancellationToken ct = default)
        where TRequest : class
        where TResponse : class;
}
