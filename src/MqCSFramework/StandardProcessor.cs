using System.Text.Json;

namespace MqCSFramework;

/// <summary>
/// Abstract base class for standard message processors.
/// Handles deserialization internally — the consumer calls ProcessRawAsync directly (no reflection).
/// Inherit this in your processor implementation.
/// </summary>
public abstract class StandardProcessor<TMessage> : IMessageProcessor<TMessage>
    where TMessage : class
{
    public Task ProcessRawAsync(ReadOnlyMemory<byte> body, MessageContext context, CancellationToken ct = default)
    {
        var message = JsonSerializer.Deserialize<TMessage>(body.Span);
        if (message is null)
        {
            throw new MessageSerializationException(
                $"Failed to deserialize message to type '{typeof(TMessage).FullName}'.",
                context.MessageId);
        }

        return ProcessAsync(message, context, ct);
    }

    public abstract Task ProcessAsync(TMessage message, MessageContext context, CancellationToken ct = default);
}
