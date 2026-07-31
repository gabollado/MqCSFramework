namespace MqCSFramework;

/// <summary>
/// Sends RPC (request-reply) messages and awaits a typed response.
/// The generic constraints enforce compile-time type safety between processor, request, and response.
/// </summary>
public interface IRpcSender
{
    Task<TResponse> SendAsync<TProcessor, TResponse, TRequest>(
        TRequest request,
        RpcOptions? options = null,
        CancellationToken ct = default)
        where TProcessor : IRpcProcessor<TRequest, TResponse>
        where TRequest : class
        where TResponse : class;
}
