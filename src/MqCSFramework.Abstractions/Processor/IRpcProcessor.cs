using MqCSFramework.Abstractions.Models;

namespace MqCSFramework.Abstractions.Processor;

/// <summary>
/// Processes an RPC request of type TRequest and returns TResponse.
/// </summary>
public interface IRpcProcessor<in TRequest, TResponse>
    where TRequest : class
    where TResponse : class
{
    Task<TResponse> ProcessAsync(TRequest request, MessageContext context, CancellationToken ct = default);
}
