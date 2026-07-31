using MqCSFramework.Abstractions.Models;

namespace MqCSFramework.Abstractions.Processor;

/// <summary>
/// Processes a standard (fire-and-forget) message of type TMessage.
/// Implement this interface and register via DI.
/// </summary>
public interface IMessageProcessor<in TMessage> where TMessage : class
{
    Task ProcessAsync(TMessage message, MessageContext context, CancellationToken ct = default);
}
