using System.Collections.Concurrent;
using System.Threading.Channels;
using MqCSFramework.Abstractions.Models;
using MqCSFramework.Abstractions.Transport;

namespace MqCSFramework.InMemory;

/// <summary>
/// In-memory channel implementation. Publish writes to a named Channel&lt;T&gt;,
/// consume reads from it in a loop invoking the handler callback.
/// ACK/NACK are no-ops (no broker to acknowledge to) but delivery tags are tracked.
/// </summary>
public sealed class InMemoryTransportChannel : ITransportChannel
{
    private readonly ConcurrentDictionary<string, Channel<MessageEnvelope>> _queues;
    private long _deliveryTagCounter;
    private CancellationTokenSource? _consumeCts;

    internal InMemoryTransportChannel(ConcurrentDictionary<string, Channel<MessageEnvelope>> queues)
    {
        _queues = queues;
    }

    public Task PublishAsync(MessageEnvelope envelope, CancellationToken ct = default)
    {
        var queueName = envelope.RoutingKey ?? "default";
        var channel = _queues.GetOrAdd(queueName, _ => Channel.CreateUnbounded<MessageEnvelope>());

        return channel.Writer.WriteAsync(envelope, ct).AsTask();
    }

    public async Task StartConsumingAsync(
        string queueName,
        Func<ReceivedMessage, Task<ProcessResult>> handler,
        CancellationToken ct = default)
    {
        var channel = _queues.GetOrAdd(queueName, _ => Channel.CreateUnbounded<MessageEnvelope>());
        _consumeCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var token = _consumeCts.Token;

        try
        {
            await foreach (var envelope in channel.Reader.ReadAllAsync(token))
            {
                var deliveryTag = (ulong)Interlocked.Increment(ref _deliveryTagCounter);

                var receivedMessage = new ReceivedMessage
                {
                    Body = envelope.Body,
                    DeliveryTag = deliveryTag,
                    MessageId = envelope.MessageId,
                    MessageType = envelope.MessageType,
                    CorrelationId = envelope.CorrelationId,
                    ReplyTo = envelope.ReplyTo,
                    Exchange = envelope.Exchange,
                    RoutingKey = envelope.RoutingKey,
                    ContentType = envelope.ContentType,
                    Timestamp = envelope.Timestamp,
                    SenderIdentity = envelope.SenderIdentity,
                    Headers = new Dictionary<string, object?>(envelope.Headers),
                    Redelivered = false
                };

                await handler(receivedMessage);
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // Graceful shutdown — consumer was stopped via cancellation.
        }
    }

    public Task AcknowledgeAsync(ulong deliveryTag, CancellationToken ct = default)
    {
        // No-op for in-memory transport — no broker to acknowledge to.
        return Task.CompletedTask;
    }

    public Task NegativeAcknowledgeAsync(ulong deliveryTag, bool requeue, CancellationToken ct = default)
    {
        // No-op for in-memory transport — no broker to negative-acknowledge to.
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _consumeCts?.Cancel();
        _consumeCts?.Dispose();
        _consumeCts = null;
        return ValueTask.CompletedTask;
    }
}
