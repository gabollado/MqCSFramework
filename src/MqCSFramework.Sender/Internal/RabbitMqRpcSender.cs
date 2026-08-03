using MqCSFramework.Internal;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace MqCSFramework.Sender.Internal;

/// <summary>
/// RPC (request-reply) sender implementation.
/// Delegates reply correlation entirely to RpcRequestResponseHandler.
/// Reply queue format: {routingKey}.reply.{GUID}
/// </summary>
internal sealed class RabbitMqRpcSender : IRpcSender, IAsyncDisposable
{
    private readonly RabbitMqConnection _connection;
    private readonly RpcSenderOptions _options;
    private readonly ILogger<RabbitMqRpcSender> _logger;
    private readonly RpcRequestResponseHandler _replyConsumer;

    public RabbitMqRpcSender(RabbitMqConnection connection, RpcSenderOptions options, ILogger<RabbitMqRpcSender> logger)
    {
        _connection = connection;
        _options = options;
        _logger = logger;

        var replyQueueName = $"{options.RoutingKey}.reply.{Guid.NewGuid():N}";
        _replyConsumer = new RpcRequestResponseHandler(connection, replyQueueName, logger);
    }

    public async Task<TResponse> SendAsync<TProcessor, TResponse, TRequest>(
        TRequest request,
        string correlationId,
        RpcOptions? options = null,
        CancellationToken ct = default)
        where TProcessor : IRpcProcessor<TRequest, TResponse>
        where TRequest : class
        where TResponse : class
    {
        var messageId = Guid.NewGuid().ToString("N");
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

        var props = new BasicProperties
        {
            MessageId = messageId,
            CorrelationId = correlationId,
            ReplyTo = _replyConsumer.ReplyQueueName,
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            ContentType = "application/json",
            Headers = new Dictionary<string, object?>
            {
                [MqHeaders.ProcessorType] = typeof(TProcessor).AssemblyQualifiedName,
                [MqHeaders.Pattern] = MqHeaders.PatternRpc,
                [MqHeaders.CancellationDeadline] = (DateTimeOffset.UtcNow + timeout).Ticks.ToString()
            }
        };

        if (options?.AdditionalHeaders is not null)
        {
            foreach (var kvp in options.AdditionalHeaders)
            {
                props.Headers[kvp.Key] = kvp.Value;
            }
        }

        _logger.LogInformation("Publishing RPC request {MessageId} for processor {Processor} to {Exchange}/{RoutingKey}",
            messageId, typeof(TProcessor).Name, _options.Exchange, routingKey);

        var responseBytes = await _replyConsumer.PublishAndAwaitReplyAsync(
            _options.Exchange, routingKey, props, body, correlationId, timeout, ct);

        // Check for error response
        var envelope = JsonSerializer.Deserialize<RpcResponseEnvelope>(responseBytes);
        if (envelope is { IsError: true })
        {
            throw new RpcRemoteException(correlationId, envelope.ErrorMessage ?? "Unknown error", envelope.ErrorType);
        }

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

    public async ValueTask DisposeAsync()
    {
        _replyConsumer.Dispose();
        await _connection.DisposeAsync();
    }
}

