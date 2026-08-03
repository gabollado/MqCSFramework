using MqCSFramework.Internal;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace MqCSFramework.Sender.Internal;

/// <summary>
/// Standard (fire-and-forget) sender implementation using RabbitMQ.
/// Each instance owns its own connection.
/// </summary>
internal sealed class RabbitMqStandardSender : IStandardSender
{
    private readonly RabbitMqConnection _connection;
    private readonly StandardSenderOptions _options;
    private readonly ILogger<RabbitMqStandardSender> _logger;

    public RabbitMqStandardSender(RabbitMqConnection connection, StandardSenderOptions options, ILogger<RabbitMqStandardSender> logger)
    {
        _connection = connection;
        _options = options;
        _logger = logger;
    }

    public async Task<string> SendAsync<TProcessor, TMessage>(
        TMessage message,
        string correlationId,
        SendOptions? options = null,
        CancellationToken ct = default)
        where TProcessor : IMessageProcessor<TMessage>
        where TMessage : class
    {
        var messageId = Guid.NewGuid().ToString("N");
        var routingKey = options?.RoutingKey ?? _options.RoutingKey;

        byte[] body;
        try
        {
            body = JsonSerializer.SerializeToUtf8Bytes(message);
        }
        catch (JsonException ex)
        {
            throw new MessageSerializationException(
                $"Failed to serialize message of type '{typeof(TMessage).FullName}'.", messageId, ex);
        }

        var props = new BasicProperties
        {
            MessageId = messageId,
            CorrelationId = correlationId,
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            ContentType = "application/json",
            Headers = new Dictionary<string, object?>
            {
                [MqHeaders.ProcessorType] = typeof(TProcessor).AssemblyQualifiedName,
                [MqHeaders.Pattern] = MqHeaders.PatternStandard
            }
        };

        if (options?.AdditionalHeaders is not null)
        {
            foreach (var kvp in options.AdditionalHeaders)
            {
                props.Headers[kvp.Key] = kvp.Value;
            }
        }

        try
        {
            var channel = await _connection.GetChannelAsync(ct);
            await channel.BasicPublishAsync(_options.Exchange, routingKey, false, props, body, ct);
        }
        catch (Exception ex) when (ex is not MessageSerializationException)
        {
            _logger.LogError(ex, "Failed to publish standard message {MessageId} to {Exchange}/{RoutingKey}",
                messageId, _options.Exchange, routingKey);
            await _connection.ResetChannelAsync();
            throw;
        }

        _logger.LogInformation("Published standard message {MessageId} for processor {Processor} to {Exchange}/{RoutingKey}",
            messageId, typeof(TProcessor).Name, _options.Exchange, routingKey);

        return messageId;
    }
}

