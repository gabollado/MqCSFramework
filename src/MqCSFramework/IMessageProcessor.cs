namespace MqCSFramework;

/// <summary>
/// Non-generic base interface for standard processors.
/// The consumer calls ProcessRawAsync — the implementation deserializes and delegates to the typed method.
/// </summary>
public interface IMessageProcessor
{
    Task ProcessRawAsync(ReadOnlyMemory<byte> body, MessageContext context, CancellationToken ct = default);
}

/// <summary>
/// Generic interface for standard message processors.
/// Define a contract interface inheriting this in your shared contracts package.
/// </summary>
public interface IMessageProcessor<in TMessage> : IMessageProcessor where TMessage : class
{
    Task ProcessAsync(TMessage message, MessageContext context, CancellationToken ct = default);
}
