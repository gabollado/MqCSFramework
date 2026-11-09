using MqCSFramework.Internal;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace MqCSFramework.Consumer.Internal;

/// <summary>
/// Manages a single consumer — owns its connection, channel, and message dispatch loop.
/// Resolves processors directly from DI using the mq-processor-type header.
/// </summary>
internal sealed class MqConsumer : IAsyncDisposable
{
    private readonly ConsumerOptions _options;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<MqConsumer> _logger;
    private readonly HashSet<string>? _maskedFields;

    private RabbitMqConnection? _connection;
    private IChannel? _channel;

    public MqConsumer(ConsumerOptions options, IServiceScopeFactory scopeFactory, ILogger<MqConsumer> logger)
    {
        _options = options;
        _scopeFactory = scopeFactory;
        _logger = logger;
        _maskedFields = options.MaskedFields.Count > 0
            ? new HashSet<string>(options.MaskedFields, StringComparer.OrdinalIgnoreCase)
            : null;
    }

    public async Task StartAsync(CancellationToken ct)
    {
        _connection = new RabbitMqConnection(_options.Connection, _logger);
        _channel = await _connection.GetChannelAsync(ct);
        await EnsureQueueAsync(ct);

        await _channel.BasicQosAsync(prefetchSize: 0, prefetchCount: _options.PrefetchCount, global: false, cancellationToken: ct);

        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.ReceivedAsync += DispatchMessageAsync;

        await _channel.BasicConsumeAsync(
            queue: _options.QueueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: ct);

        _logger.LogInformation("Consumer started on queue '{QueueName}' with prefetch {PrefetchCount}",
            _options.QueueName, _options.PrefetchCount);
    }

    /// <summary>
    /// Uses an existing queue as it is (any type or arguments, so no 406 on redeclare);
    /// only a missing queue is declared, as a quorum queue.
    /// </summary>
    private async Task EnsureQueueAsync(CancellationToken ct)
    {
        try
        {
            await _channel!.QueueDeclarePassiveAsync(_options.QueueName, ct);
            _logger.LogInformation("Queue '{QueueName}' exists; using it as it is", _options.QueueName);
            return;
        }
        catch (OperationInterruptedException ex) when (ex.ShutdownReason?.ReplyCode == Constants.NotFound)
        {
            // The broker closes the channel on 404, so declare on a new one.
        }

        await _connection!.ResetChannelAsync();
        _channel = await _connection.GetChannelAsync(ct);

        await _channel.QueueDeclareAsync(
            queue: _options.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-queue-type"] = "quorum" },
            cancellationToken: ct);
        _logger.LogInformation("Queue '{QueueName}' declared as a quorum queue", _options.QueueName);
    }

    private async Task DispatchMessageAsync(object sender, BasicDeliverEventArgs ea)
    {
        var messageId = ea.BasicProperties?.MessageId ?? "unknown";
        var correlationId = ea.BasicProperties?.CorrelationId ?? messageId;

        // Wrap all processing in a logging scope so every log entry includes CorrelationId
        using (_logger.CorrelationScope(correlationId))
        {
            await DispatchMessageCoreAsync(ea, messageId, correlationId);
        }
    }

    private async Task DispatchMessageCoreAsync(BasicDeliverEventArgs ea, string messageId, string correlationId)
    {
        try
        {
            // 1. Read mq-processor-type header
            var processorTypeName = MessageHelpers.GetHeaderString(ea, MqHeaders.ProcessorType);
            if (processorTypeName is null)
            {
                _logger.LogWarning("Message {MessageId} missing '{Header}' header. NACK without requeue.",
                    messageId, MqHeaders.ProcessorType);
                await NackWithoutRequeueAsync(ea);
                return;
            }

            // 2. Resolve processor type
            var processorType = Type.GetType(processorTypeName);
            if (processorType is null)
            {
                _logger.LogError("Cannot resolve processor type '{ProcessorType}' for message {MessageId}. NACK without requeue.",
                    processorTypeName, messageId);
                await NackWithoutRequeueAsync(ea);
                return;
            }

            // 3. Resolve processor from its own DI scope so scoped dependencies
            // (e.g. DbContext) are not shared across messages. The scope is disposed
            // when message processing finishes.
            using var scope = _scopeFactory.CreateScope();
            var processor = scope.ServiceProvider.GetService(processorType);
            if (processor is null)
            {
                _logger.LogError("Processor '{ProcessorType}' not registered in DI for message {MessageId}. NACK without requeue.",
                    processorType.FullName, messageId);
                await NackWithoutRequeueAsync(ea);
                return;
            }

            // 4. Determine pattern
            var pattern = MessageHelpers.GetHeaderString(ea, MqHeaders.Pattern);
            if (pattern is null)
            {
                _logger.LogWarning("Message {MessageId} missing '{Header}' header. NACK without requeue.",
                    messageId, MqHeaders.Pattern);
                await NackWithoutRequeueAsync(ea);
                return;
            }

            // 5. Log message body (masked or suppressed)
            LogMessageBody(ea, messageId);

            var context = MessageHelpers.BuildContext(ea, messageId, correlationId, pattern);

            // 6. Dispatch based on pattern
            if (pattern == MqHeaders.PatternRpc)
            {
                await DispatchRpcAsync(ea, processor, processorType, context);
                return;
            }

            await DispatchStandardAsync(ea, processor, processorType, context);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unhandled error processing message {MessageId}. Applying retry logic.", messageId);
            try
            {
                await HandleFailureAsync(ea, messageId);
            }
            catch (Exception failureEx)
            {
                _logger.LogError(failureEx, "Failed to handle failure for message {MessageId}. NACK without requeue.", messageId);
                await NackWithoutRequeueAsync(ea);
            }
        }
    }

    private async Task DispatchStandardAsync(BasicDeliverEventArgs ea, object processor, Type processorType, MessageContext context)
    {
        if (processor is not IMessageProcessor standardProcessor)
        {
            _logger.LogError("Processor {ProcessorType} does not implement IMessageProcessor for message {MessageId}. NACK without requeue.",
                processorType.FullName, context.MessageId);
            await NackWithoutRequeueAsync(ea);
            return;
        }

        await standardProcessor.ProcessRawAsync(ea.Body, context, CreateStandardTimeoutToken());

        await _channel!.BasicAckAsync(ea.DeliveryTag, multiple: false);
        _logger.LogInformation("Message {MessageId} processed successfully by {Processor}. ACK.",
            context.MessageId, processorType.Name);
    }

    private async Task DispatchRpcAsync(BasicDeliverEventArgs ea, object processor, Type processorType, MessageContext context)
    {
        if (processor is not IRpcProcessor rpcProcessor)
        {
            _logger.LogError("Processor {ProcessorType} does not implement IRpcProcessor for message {MessageId}. NACK without requeue.",
                processorType.FullName, context.MessageId);
            await NackWithoutRequeueAsync(ea);
            return;
        }

        // The processor produces the serialized TResponse bytes. Business failures are carried
        // inside that response (RpcResponse.Error) — they are not exceptions. An unexpected
        // exception is intentionally NOT caught here: it propagates to DispatchMessageCoreAsync,
        // which routes it to HandleFailureAsync (NACK/retry/dead-letter). No reply is published
        // in that case, so the sender times out.
        var responseBytes = await rpcProcessor.ProcessRawRpcAsync(ea.Body, context, CreateRpcTimeoutToken(ea));

        // Publish the serialized TResponse directly to ReplyTo (no wrapping envelope).
        var replyTo = ea.BasicProperties?.ReplyTo;
        if (!string.IsNullOrEmpty(replyTo))
        {
            var replyProps = new BasicProperties
            {
                // Echo the request's MessageId — the sender matches the reply to its pending call by this.
                MessageId = ea.BasicProperties?.MessageId,
                // Echo CorrelationId for trace continuity (not used for matching).
                CorrelationId = ea.BasicProperties?.CorrelationId,
                ContentType = "application/json"
                // Left transient: the reply queue is exclusive and non-durable, so persistence buys nothing.
            };

            try
            {
                await _channel!.BasicPublishAsync("", replyTo, _connection!.ConfirmPublish, replyProps, responseBytes);
            }
            catch (PublishException ex)
            {
                // Only with ConfirmPublish, e.g. the reply queue is gone because the sender stopped. The request
                // was processed, so don't hand it to the retry path: that would process it again.
                _logger.LogWarning(ex, "Reply to {ReplyTo} for message {MessageId} was not accepted by the broker. ACK anyway.",
                    replyTo, context.MessageId);
            }
        }

        await _channel!.BasicAckAsync(ea.DeliveryTag, multiple: false);
    }

    private async Task HandleFailureAsync(BasicDeliverEventArgs ea, string messageId)
    {
        if (!_options.Retry)
        {
            // Dead-lettered through the queue's dead-letter exchange, or dropped when it has none.
            await _channel!.BasicRejectAsync(ea.DeliveryTag, requeue: false);
            _logger.LogWarning("Message {MessageId} failed. Rejected without requeue (dead-lettered if the queue has a dead-letter exchange).", messageId);
            return;
        }

        // basic.reject (not nack) increments the quorum queue's delivery count; the policy applies delay,
        // delivery-limit and dead-lettering. On a classic queue the message comes back immediately.
        await _channel!.BasicRejectAsync(ea.DeliveryTag, requeue: true);
        _logger.LogWarning("Message {MessageId} failed. Rejected with requeue for broker retry.", messageId);
    }

    private void LogMessageBody(BasicDeliverEventArgs ea, string messageId)
    {
        var bodyString = Encoding.UTF8.GetString(ea.Body.Span);

        if (_maskedFields is not null && _maskedFields.Count > 0)
        {
            _logger.LogDebug("Message {MessageId} body: {Body}", messageId, LogMaskingHelper.Mask(bodyString, _maskedFields));
            return;
        }

        _logger.LogDebug("Message {MessageId} body: {Body}", messageId, bodyString);
    }

    private CancellationToken CreateStandardTimeoutToken()
    {
        if (_options.ProcessingTimeoutMs <= 0)
        {
            return CancellationToken.None;
        }

        var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(_options.ProcessingTimeoutMs));
        return cts.Token;
    }

    private CancellationToken CreateRpcTimeoutToken(BasicDeliverEventArgs ea)
    {
        var deadlineStr = MessageHelpers.GetHeaderString(ea, MqHeaders.CancellationDeadline);
        if (deadlineStr is null || !long.TryParse(deadlineStr, out var deadlineTicks))
        {
            return CancellationToken.None;
        }

        var deadline = new DateTimeOffset(deadlineTicks, TimeSpan.Zero);
        var remaining = deadline - DateTimeOffset.UtcNow;

        if (remaining <= TimeSpan.Zero)
        {
            // Already expired — cancel immediately
            return new CancellationToken(canceled: true);
        }

        var cts = new CancellationTokenSource(remaining);
        return cts.Token;
    }

    private async Task NackWithoutRequeueAsync(BasicDeliverEventArgs ea)
    {
        if (_channel is not null)
        {
            await _channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection is not null)
        {
            await _connection.DisposeAsync();
        }
    }
}

