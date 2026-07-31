using MqCSFramework.Abstractions.Configuration;

namespace MqCSFramework.Abstractions.Sender;

/// <summary>
/// RPC sender. Publishes a request and awaits a typed response.
/// Always targets a specific processor contract interface for compile-time routing.
/// </summary>
public interface IRpcSender
{
    /// <summary>
    /// Send an RPC request targeting a specific processor contract interface.
    /// TProcessor must be a processor contract interface (e.g., ICheckStockProcessor : IRpcProcessor&lt;CheckStockRequest, CheckStockResponse&gt;).
    /// TResponse is the response type defined by the processor interface.
    /// The interface's full type name is set as the mq-processor-type header for routing.
    /// </summary>
    Task<TResponse> SendAsync<TProcessor, TResponse>(
        object request,
        RpcOptions? options = null,
        CancellationToken ct = default)
        where TProcessor : class
        where TResponse : class;
}
