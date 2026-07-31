namespace MqCSFramework;

/// <summary>
/// Sends standard (fire-and-forget) messages.
/// The generic constraints enforce compile-time type safety between processor and message.
/// </summary>
public interface IStandardSender
{
    Task<string> SendAsync<TProcessor, TMessage>(
        TMessage message,
        SendOptions? options = null,
        CancellationToken ct = default)
        where TProcessor : IMessageProcessor<TMessage>
        where TMessage : class;
}
