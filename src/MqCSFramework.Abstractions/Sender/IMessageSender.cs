using MqCSFramework.Abstractions.Configuration;

namespace MqCSFramework.Abstractions.Sender;

/// <summary>
/// Fire-and-forget message sender. Publishes a message without expecting a response.
/// </summary>
public interface IMessageSender
{
    /// <summary>
    /// Send a message specifying the target processor type.
    /// The processor type is added as a header for routing on the consumer side.
    /// TProcessor can be the concrete processor class or a shared contract interface.
    /// </summary>
    Task<string> SendAsync<TProcessor>(
        object message,
        SendOptions? options = null,
        CancellationToken ct = default)
        where TProcessor : class;

    /// <summary>
    /// Send a message without specifying a processor (routes by message type name on consumer side).
    /// </summary>
    Task<string> SendAsync<TMessage>(
        TMessage message,
        SendOptions? options = null,
        CancellationToken ct = default) where TMessage : class;
}
