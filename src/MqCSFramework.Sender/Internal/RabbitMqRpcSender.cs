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
        where TResponse : RpcResponse
    {
        var routingKey = options?.RoutingKey ?? _options.RoutingKey;
        var timeout = TimeSpan.FromMilliseconds(options?.TimeoutMs ?? _options.TimeoutMs);
        var maxExceptionRetries = options?.MaxExceptionRetries ?? _options.MaxExceptionRetries;
        var exceptionRetryDelayMs = options?.ExceptionRetryDelayMs ?? _options.ExceptionRetryDelayMs;

        var body = JsonSerializer.SerializeToUtf8Bytes(request);

        var attempt = 0;
        
        do
        {
            try
            {
                attempt++;
                return await SendAttemptAsync<TProcessor, TResponse, TRequest>(
                    body, correlationId, routingKey, timeout, options, attempt, ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException && attempt <= maxExceptionRetries)
            {
                _logger.LogWarning(ex,
                    "RPC request for processor {Processor} failed on attempt {Attempt} of {Total}. Retrying in {Delay}ms.",
                    typeof(TProcessor).Name, attempt, maxExceptionRetries + 1, exceptionRetryDelayMs);

                await Task.Delay(exceptionRetryDelayMs, ct);
            }
        }
        while (attempt <= maxExceptionRetries);

        throw new Exception("Max retries reached");
    }

    private async Task<TResponse> SendAttemptAsync<TProcessor, TResponse, TRequest>(
        byte[] body,
        string correlationId,
        string routingKey,
        TimeSpan timeout,
        RpcOptions? options,
        int attempt,
        CancellationToken ct)
        where TProcessor : IRpcProcessor<TRequest, TResponse>
        where TRequest : class
        where TResponse : RpcResponse
    {
        var messageId = Guid.NewGuid().ToString("N");

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

        _logger.LogInformation(
            "Publishing RPC request {MessageId} for processor {Processor} to {Exchange}/{RoutingKey} (attempt {Attempt})",
            messageId, typeof(TProcessor).Name, _options.Exchange, routingKey, attempt);

        var responseBytes = await _replyConsumer.PublishAndAwaitReplyAsync(
            _options.Exchange, routingKey, props, body, messageId, correlationId, timeout, ct);

        var response = JsonSerializer.Deserialize<TResponse>(responseBytes) ?? throw new MessageSerializationException($"Failed to deserialize RPC response to type '{typeof(TResponse).FullName}'.", messageId);
        return response;
    }

    public async ValueTask DisposeAsync()
    {
        _replyConsumer.Dispose();
        await _connection.DisposeAsync();
    }
}

