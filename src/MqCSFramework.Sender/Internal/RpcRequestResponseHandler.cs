using MqCSFramework.Internal;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MqCSFramework.Sender.Internal;

/// <summary>
/// Manages the reply queue consumer for RPC responses.
/// Owns the pending request dictionary and handles the full correlation lifecycle:
/// ensure started, register pending, publish, await reply, timeout, cleanup.
/// </summary>
internal sealed class RpcRequestResponseHandler : IDisposable
{
    private readonly RabbitMqConnection _connection;
    private readonly string _replyQueueName;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private readonly ConcurrentDictionary<string, TaskCompletionSource<byte[]>> _pending = new();
    private bool _started;

    public string ReplyQueueName => _replyQueueName;

    public RpcRequestResponseHandler(RabbitMqConnection connection, string replyQueueName, ILogger logger)
    {
        _connection = connection;
        _replyQueueName = replyQueueName;
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
        await EnsureStartedAsync(ct);

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
            var channel = await _connection.GetChannelAsync(ct);
            await channel.BasicPublishAsync(exchange, routingKey, false, props, body, ct);

            return await tcs.Task;
        }
        catch (Exception) when (!tcs.Task.IsCompleted)
        {
            _pending.TryRemove(messageId, out _);
            throw;
        }
    }

    private async Task EnsureStartedAsync(CancellationToken ct)
    {
        if (_started)
        {
            return;
        }

        await _initLock.WaitAsync(ct);
        try
        {
            if (_started)
            {
                return;
            }

            var channel = await _connection.GetChannelAsync(ct);

            await channel.QueueDeclareAsync(
                queue: _replyQueueName,
                durable: false,
                exclusive: true,
                autoDelete: true,
                arguments: null,
                cancellationToken: ct);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += HandleReplyAsync;

            await channel.BasicConsumeAsync(_replyQueueName, autoAck: true, consumer: consumer, cancellationToken: ct);
            _started = true;

            _logger.LogInformation("RPC reply consumer started on queue '{ReplyQueue}'", _replyQueueName);
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

