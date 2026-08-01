using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MqCSFramework.Internal;

/// <summary>
/// Manages a single consumer — owns its connection, channel, and message dispatch loop.
/// Resolves processors directly from DI using the mq-processor-type header.
/// </summary>
internal sealed class MqConsumer : IAsyncDisposable
{
    private readonly ConsumerOptions _options;
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<MqConsumer> _logger;
    private readonly HashSet<string>? _maskedFields;

    private RabbitMqConnection? _connection;
    private IChannel? _channel;

    public MqConsumer(ConsumerOptions options, IServiceProvider serviceProvider, ILogger<MqConsumer> logger)
    {
        _options = options;
        _serviceProvider = serviceProvider;
        _logger = logger;
        _maskedFields = options.MaskedFields.Count > 0
            ? new HashSet<string>(options.MaskedFields, StringComparer.OrdinalIgnoreCase)
            : null;
    }

    public async Task StartAsync(CancellationToken ct)
    {
        _connection = new RabbitMqConnection(_options.Connection, _logger);
        _channel = await _connection.GetChannelAsync(ct);

        // Declare queue (idempotent — creates if not exists, no-op if already exists)
        await _channel.QueueDeclareAsync(
            queue: _options.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: ct);

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

    private async Task DispatchMessageAsync(object sender, BasicDeliverEventArgs ea)
    {
        var messageId = ea.BasicProperties?.MessageId ?? "unknown";
        var correlationId = ea.BasicProperties?.CorrelationId ?? messageId;

        try
        {
            // 1. Read mq-processor-type header
            var processorTypeName = GetHeaderString(ea, MqHeaders.ProcessorType);
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

            // 3. Resolve processor from DI
            var processor = _serviceProvider.GetService(processorType);
            if (processor is null)
            {
                _logger.LogError("Processor '{ProcessorType}' not registered in DI for message {MessageId}. NACK without requeue.",
                    processorType.FullName, messageId);
                await NackWithoutRequeueAsync(ea);
                return;
            }

            // 4. Determine pattern
            var pattern = GetHeaderString(ea, MqHeaders.Pattern);
            if (pattern is null)
            {
                _logger.LogWarning("Message {MessageId} missing '{Header}' header. NACK without requeue.",
                    messageId, MqHeaders.Pattern);
                await NackWithoutRequeueAsync(ea);
                return;
            }

            // 5. Log message body (masked or suppressed)
            LogMessageBody(ea, messageId);

            var context = BuildContext(ea, messageId, correlationId, pattern);

            // 6. Dispatch based on pattern
            if (pattern == MqHeaders.PatternRpc)
            {
                await DispatchRpcAsync(ea, processor, processorType, context);
            }
            else
            {
                await DispatchStandardAsync(ea, processor, processorType, context);
            }
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

        await standardProcessor.ProcessRawAsync(ea.Body, context, CancellationToken.None);

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

        RpcResponseEnvelope envelope;
        try
        {
            var responseBytes = await rpcProcessor.ProcessRawRpcAsync(ea.Body, context, CancellationToken.None);
            envelope = new RpcResponseEnvelope { IsError = false, Payload = responseBytes };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "RPC processor {ProcessorType} threw for message {MessageId}. Returning error response.",
                processorType.FullName, context.MessageId);

            var innerEx = ex.InnerException ?? ex;
            envelope = new RpcResponseEnvelope
            {
                IsError = true,
                ErrorMessage = innerEx.Message,
                ErrorType = innerEx.GetType().FullName
            };
        }

        // Publish response to ReplyTo
        var replyTo = ea.BasicProperties?.ReplyTo;
        if (!string.IsNullOrEmpty(replyTo))
        {
            var responseBody = JsonSerializer.SerializeToUtf8Bytes(envelope);
            var replyProps = new BasicProperties
            {
                CorrelationId = ea.BasicProperties?.CorrelationId,
                ContentType = "application/json"
            };

            await _channel!.BasicPublishAsync("", replyTo, false, replyProps, responseBody);
        }

        await _channel!.BasicAckAsync(ea.DeliveryTag, multiple: false);
    }

    private async Task HandleFailureAsync(BasicDeliverEventArgs ea, string messageId)
    {
        var retryCount = GetRetryCount(ea);

        if (_options.MaxRetries > 0 && retryCount >= _options.MaxRetries)
        {
            // Dead-letter: publish to DLX if configured, then ACK original
            if (!string.IsNullOrEmpty(_options.DeadLetterExchange))
            {
                _logger.LogWarning("Message {MessageId} exceeded max retries ({MaxRetries}). Routing to dead-letter.",
                    messageId, _options.MaxRetries);

                var dlProps = new BasicProperties
                {
                    MessageId = ea.BasicProperties?.MessageId,
                    CorrelationId = ea.BasicProperties?.CorrelationId,
                    ContentType = ea.BasicProperties?.ContentType,
                    Headers = ea.BasicProperties?.Headers != null
                        ? new Dictionary<string, object?>(ea.BasicProperties.Headers)
                        : new Dictionary<string, object?>()
                };

                await _channel!.BasicPublishAsync(
                    _options.DeadLetterExchange,
                    _options.DeadLetterRoutingKey ?? "",
                    false, dlProps, ea.Body, CancellationToken.None);

                await _channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
            }
            else
            {
                // No DLX configured — NACK without requeue
                await NackWithoutRequeueAsync(ea);
            }
        }
        else
        {
            // NACK with requeue — increment retry header
            // Note: RabbitMQ's built-in NACK+requeue doesn't allow modifying headers on the same message,
            // so we republish with updated header and ACK the original
            var headers = ea.BasicProperties?.Headers != null
                ? new Dictionary<string, object?>(ea.BasicProperties.Headers)
                : new Dictionary<string, object?>();
            headers[MqHeaders.RetryCount] = retryCount + 1;

            var retryProps = new BasicProperties
            {
                MessageId = ea.BasicProperties?.MessageId,
                CorrelationId = ea.BasicProperties?.CorrelationId,
                Timestamp = ea.BasicProperties?.Timestamp ?? new AmqpTimestamp(0),
                ContentType = ea.BasicProperties?.ContentType,
                ReplyTo = ea.BasicProperties?.ReplyTo,
                Headers = headers
            };

            await _channel!.BasicPublishAsync(
                ea.Exchange ?? "",
                ea.RoutingKey,
                false, retryProps, ea.Body, CancellationToken.None);

            await _channel.BasicAckAsync(ea.DeliveryTag, multiple: false);

            _logger.LogWarning("Message {MessageId} failed (retry {RetryCount}/{MaxRetries}). Requeued.",
                messageId, retryCount + 1, _options.MaxRetries);
        }
    }

    private void LogMessageBody(BasicDeliverEventArgs ea, string messageId)
    {
        if (_options.SuppressMessageBodyLogging)
        {
            return;
        }

        var bodyString = Encoding.UTF8.GetString(ea.Body.Span);

        if (_maskedFields is not null && _maskedFields.Count > 0)
        {
            var maskedBody = LogMaskingHelper.Mask(bodyString, _maskedFields);
            _logger.LogDebug("Message {MessageId} body: {Body}", messageId, maskedBody);
        }
        else
        {
            _logger.LogDebug("Message {MessageId} body: {Body}", messageId, bodyString);
        }
    }

    private static int GetRetryCount(BasicDeliverEventArgs ea)
    {
        if (ea.BasicProperties?.Headers is null)
        {
            return 0;
        }

        if (!ea.BasicProperties.Headers.TryGetValue(MqHeaders.RetryCount, out var value))
        {
            return 0;
        }

        return value switch
        {
            int i => i,
            long l => (int)l,
            byte[] bytes => int.TryParse(Encoding.UTF8.GetString(bytes), out var parsed) ? parsed : 0,
            _ => 0
        };
    }

    private async Task NackWithoutRequeueAsync(BasicDeliverEventArgs ea)
    {
        if (_channel is not null)
        {
            await _channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
        }
    }

    private static string? GetHeaderString(BasicDeliverEventArgs ea, string headerName)
    {
        if (ea.BasicProperties?.Headers is null)
        {
            return null;
        }

        if (!ea.BasicProperties.Headers.TryGetValue(headerName, out var value))
        {
            return null;
        }

        return value switch
        {
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            string s => s,
            _ => value?.ToString()
        };
    }

    private static MessageContext BuildContext(BasicDeliverEventArgs ea, string messageId, string correlationId, string pattern)
    {
        var headers = new Dictionary<string, string>();
        if (ea.BasicProperties?.Headers is not null)
        {
            foreach (var kvp in ea.BasicProperties.Headers)
            {
                var val = kvp.Value switch
                {
                    byte[] bytes => Encoding.UTF8.GetString(bytes),
                    _ => kvp.Value?.ToString() ?? ""
                };
                headers[kvp.Key] = val;
            }
        }

        var timestamp = ea.BasicProperties?.Timestamp.UnixTime > 0
            ? DateTimeOffset.FromUnixTimeSeconds(ea.BasicProperties.Timestamp.UnixTime)
            : DateTimeOffset.UtcNow;

        return new MessageContext
        {
            MessageId = messageId,
            CorrelationId = correlationId,
            Timestamp = timestamp,
            Pattern = pattern,
            Headers = headers
        };
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection is not null)
        {
            await _connection.DisposeAsync();
        }
    }
}
