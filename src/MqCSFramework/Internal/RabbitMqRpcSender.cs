using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MqCSFramework.Internal;

/// <summary>
/// RPC (request-reply) sender implementation using a named exclusive reply queue.
/// Each instance owns its own connection and manages pending request correlation.
/// Reply queue format: {routingKey}.reply.{GUID}
/// </summary>
internal sealed class RabbitMqRpcSender : IRpcSender, IAsyncDisposable
{
    private readonly RabbitMqConnection _connection;
    private readonly RpcSenderOptions _options;
    private readonly ILogger<RabbitMqRpcSender> _logger;
    private readonly ConcurrentDictionary<string, TaskCompletionSource<byte[]>> _pending = new();
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private readonly string _replyQueueName;
    private bool _replyConsumerStarted;

    public RabbitMqRpcSender(RabbitMqConnection connection, RpcSenderOptions options, ILogger<RabbitMqRpcSender> logger)
    {
        _connection = connection;
        _options = options;
        _logger = logger;
        _replyQueueName = $"{options.RoutingKey}.reply.{Guid.NewGuid():N}";
    }

    public async Task<TResponse> SendAsync<TProcessor, TResponse, TRequest>(
        TRequest request,
        RpcOptions? options = null,
        CancellationToken ct = default)
        where TProcessor : IRpcProcessor<TRequest, TResponse>
        where TRequest : class
        where TResponse : class
    {
        var messageId = Guid.NewGuid().ToString();
        var correlationId = options?.CorrelationId ?? messageId;
        var routingKey = options?.RoutingKey ?? _options.RoutingKey;
        var timeout = options?.Timeout ?? _options.Timeout;

        byte[] body;
        try
        {
            body = JsonSerializer.SerializeToUtf8Bytes(request);
        }
        catch (JsonException ex)
        {
            throw new MessageSerializationException(
                $"Failed to serialize RPC request of type '{typeof(TRequest).FullName}'.", messageId, ex);
        }

        var tcs = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pending[correlationId] = tcs;

        try
        {
            await EnsureReplyConsumerAsync(ct);

            var channel = await _connection.GetChannelAsync(ct);

            var props = new BasicProperties
            {
                MessageId = messageId,
                CorrelationId = correlationId,
                ReplyTo = _replyQueueName,
                Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
                ContentType = "application/json",
                Headers = new Dictionary<string, object?>
                {
                    [MqHeaders.ProcessorType] = typeof(TProcessor).AssemblyQualifiedName,
                    [MqHeaders.Pattern] = MqHeaders.PatternRpc
                }
            };

            if (options?.AdditionalHeaders is not null)
            {
                foreach (var kvp in options.AdditionalHeaders)
                {
                    props.Headers[kvp.Key] = kvp.Value;
                }
            }

            await channel.BasicPublishAsync(_options.Exchange, routingKey, false, props, body, ct);

            _logger.LogInformation("Published RPC request {MessageId} for processor {Processor} to {Exchange}/{RoutingKey}",
                messageId, typeof(TProcessor).Name, _options.Exchange, routingKey);

            // Await response with timeout
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(timeout);

            byte[] responseBytes;
            try
            {
                responseBytes = await tcs.Task.WaitAsync(cts.Token);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                throw new RpcTimeoutException(correlationId, timeout);
            }

            // Check for error response
            var envelope = JsonSerializer.Deserialize<RpcResponseEnvelope>(responseBytes);
            if (envelope is { IsError: true })
            {
                throw new RpcRemoteException(correlationId, envelope.ErrorMessage ?? "Unknown error", envelope.ErrorType);
            }

            // Deserialize actual response from payload
            if (envelope?.Payload is null)
            {
                throw new MessageSerializationException("RPC response payload was null.", messageId);
            }

            var response = JsonSerializer.Deserialize<TResponse>(envelope.Payload);
            if (response is null)
            {
                throw new MessageSerializationException(
                    $"Failed to deserialize RPC response to type '{typeof(TResponse).FullName}'.", messageId);
            }

            return response;
        }
        finally
        {
            _pending.TryRemove(correlationId, out _);
        }
    }

    private async Task EnsureReplyConsumerAsync(CancellationToken ct)
    {
        if (_replyConsumerStarted)
        {
            return;
        }

        await _initLock.WaitAsync(ct);
        try
        {
            if (_replyConsumerStarted)
            {
                return;
            }

            var channel = await _connection.GetChannelAsync(ct);

            // Declare exclusive, auto-delete reply queue
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
            _replyConsumerStarted = true;

            _logger.LogInformation("RPC reply consumer started on queue '{ReplyQueue}'", _replyQueueName);
        }
        finally
        {
            _initLock.Release();
        }
    }

    private Task HandleReplyAsync(object sender, BasicDeliverEventArgs ea)
    {
        var correlationId = ea.BasicProperties?.CorrelationId;
        if (correlationId is null)
        {
            return Task.CompletedTask;
        }

        if (_pending.TryRemove(correlationId, out var tcs))
        {
            tcs.TrySetResult(ea.Body.ToArray());
        }

        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        // Cancel all pending requests
        foreach (var kvp in _pending)
        {
            if (_pending.TryRemove(kvp.Key, out var tcs))
            {
                tcs.TrySetCanceled();
            }
        }

        await _connection.DisposeAsync();
        _initLock.Dispose();
    }
}
