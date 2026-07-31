using MqCSFramework.Abstractions.Configuration;

namespace MqCSFramework.Abstractions.Sender;

/// <summary>
/// Fire-and-forget message sender.
/// Always targets a specific processor contract interface for compile-time routing.
/// </summary>
public interface IMessageSender
{
    /// <summary>
    /// Send a message targeting a specific processor contract interface.
    /// TProcessor must be a processor contract interface (e.g., IOrderPlacedProcessor : IMessageProcessor&lt;OrderPlaced&gt;).
    /// The interface's full type name is set as the mq-processor-type header for routing.
    /// </summary>
    Task<string> SendAsync<TProcessor>(
        object message,
        SendOptions? options = null,
        CancellationToken ct = default)
        where TProcessor : class;
}
