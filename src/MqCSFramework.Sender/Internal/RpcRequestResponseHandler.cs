using MqCSFramework.Internal;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MqCSFramework.Sender.Internal;

/// <summary>
/// Manages the reply queue consumer for RPC responses.
/// Owns the pending request dictionary and handles the full correlation lifecycle:
/// ensure the reply consumer, register pending, publish, await reply, timeout, cleanup.
/// </summary>
internal sealed class RpcRequestResponseHandler : IDisposable
{
    /// <summary>The channel the reply consumer is registered on and the reply queue it consumes.</summary>
    private sealed record ReplyRegistration(IChannel Channel, string QueueName);

    private readonly RabbitMqConnection _connection;
    private readonly string _replyQueuePrefix;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private readonly ConcurrentDictionary<string, TaskCompletionSource<byte[]>> _pending = new();

    // Replaced as one reference, so a reader never pairs a channel with another channel's queue name.
    private volatile ReplyRegistration? _registration;

    public RpcRequestResponseHandler(RabbitMqConnection connection, string replyQueuePrefix, ILogger logger)
    {
        _connection = connection;
        _replyQueuePrefix = replyQueuePrefix;
        _logger = logger;
    }

    /// <summary>
    /// Registers a pending request, ensures the consumer is started, publishes the message,
    /// and awaits the response. Handles timeout internally.
    /// </summary>
    public async Task<byte[]> PublishAndAwaitReplyAsync(
        string exchange,
        string routingKey,
        BasicProperties props,
        byte[] body,
        string messageId,
        string correlationId,
        TimeSpan timeout,
        CancellationToken ct)
    {
        var reply = await EnsureReplyConsumerAsync(ct);
        props.ReplyTo = reply.QueueName;

        // Pending calls are keyed by the per-message MessageId, which is unique per message and per
        // retry attempt. CorrelationId is NOT used for matching — it is a process-level trace id that
        // can be shared across multiple messages, so two concurrent calls could collide under it.
        var tcs = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pending[messageId] = tcs;

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout);

        using var registration = cts.Token.Register(() =>
        {
            if (!_pending.TryRemove(messageId, out var pendingTcs))
            {
                return;
            }

            // The linked token fires on either the caller's cancellation or the timeout.
            // Surface a cancellation as OperationCanceledException (not a retryable timeout) so
            // callers and the sender's exception-retry logic can tell the two apart.
            if (ct.IsCancellationRequested)
            {
                pendingTcs.TrySetCanceled(ct);
                return;
            }

            pendingTcs.TrySetException(new RpcTimeoutException(correlationId, timeout));
        });

        try
        {
            // Publish on the channel the reply consumer listens on. With ConfirmPublish this also waits for
            // the broker's ack; a returned (no queue matched) or rejected request throws PublishException,
            // which the sender does not retry.
            await reply.Channel.BasicPublishAsync(exchange, routingKey, _connection.ConfirmPublish, props, body, ct);

            return await tcs.Task;
        }
        catch (Exception) when (!tcs.Task.IsCompleted)
        {
            _pending.TryRemove(messageId, out _);
            throw;
        }
    }

    /// <summary>
    /// Returns the channel to publish on and the reply queue for ReplyTo. Registers the reply consumer on
    /// first use, and again whenever the connection hands out a different channel: the old consumer died
    /// with its channel, and its auto-delete queue with it. A channel the client recovers automatically
    /// stays the same instance (and the client re-registers its consumer), so it is left alone.
    /// </summary>
    private async Task<ReplyRegistration> EnsureReplyConsumerAsync(CancellationToken ct)
    {
        var channel = await _connection.GetChannelAsync(ct);
        var current = _registration;
        if (current is not null && ReferenceEquals(current.Channel, channel))
        {
            return current;
        }

        await _initLock.WaitAsync(ct);
        try
        {
            channel = await _connection.GetChannelAsync(ct);
            current = _registration;
            if (current is not null && ReferenceEquals(current.Channel, channel))
            {
                return current;
            }

            // A new name every time: if the previous connection died without a clean close, the broker can
            // keep its exclusive queue until it notices, and redeclaring that name would fail.
            var queueName = $"{_replyQueuePrefix}{Guid.NewGuid():N}";

            await channel.QueueDeclareAsync(
                queue: queueName,
                durable: false,
                exclusive: true,
                autoDelete: true,
                arguments: null,
                cancellationToken: ct);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += HandleReplyAsync;

            await channel.BasicConsumeAsync(queueName, autoAck: true, consumer: consumer, cancellationToken: ct);

            var registration = new ReplyRegistration(channel, queueName);
            _registration = registration;

            if (current is null)
            {
                _logger.LogInformation("RPC reply consumer started on queue '{ReplyQueue}'", queueName);
                return registration;
            }

            _logger.LogWarning("RPC channel was replaced; reply consumer re-registered on queue '{ReplyQueue}' (was '{OldReplyQueue}')",
                queueName, current.QueueName);
            return registration;
        }
        finally
        {
            _initLock.Release();
        }
    }

    private Task HandleReplyAsync(object sender, BasicDeliverEventArgs ea)
    {
        // Match the reply to its pending request by MessageId (the per-message match key).
        // An unmatched reply (e.g. a late reply to an attempt that already timed out and was removed)
        // is silently discarded.
        var messageId = ea.BasicProperties?.MessageId;
        if (messageId is null)
        {
            return Task.CompletedTask;
        }

        if (_pending.TryRemove(messageId, out var tcs))
        {
            tcs.TrySetResult(ea.Body.ToArray());
        }

        return Task.CompletedTask;
    }

    public void Dispose()
    {
        foreach (var kvp in _pending)
        {
            if (_pending.TryRemove(kvp.Key, out var tcs))
            {
                tcs.TrySetCanceled();
            }
        }

        _initLock.Dispose();
    }
}

