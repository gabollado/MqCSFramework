using Microsoft.Extensions.Logging;
using MqCSFramework.Abstractions.Consumer;
using MqCSFramework.Abstractions.Internal;
using MqCSFramework.Abstractions.Models;
using MqCSFramework.Abstractions.Transport;

namespace MqCSFramework.InMemory;

/// <summary>
/// In-memory consumer that reads from an in-process channel and dispatches messages
/// via MessageDispatcher. Suitable for testing and local development.
/// </summary>
public sealed class InMemoryConsumer : IMessageConsumer
{
    private readonly InMemoryTransportConnection _connection;
    private readonly MessageDispatcher _dispatcher;
    private readonly ILogger<InMemoryConsumer> _logger;
    private readonly string _queueName;

    private ITransportChannel? _channel;
    private CancellationTokenSource? _consumeCts;
    private Task? _consumeTask;

    public string QueueName => _queueName;
    public bool IsRunning { get; private set; }

    internal InMemoryConsumer(
        InMemoryTransportConnection connection,
        MessageDispatcher dispatcher,
        string queueName,
        ILogger<InMemoryConsumer> logger)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(dispatcher);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);

        _connection = connection;
        _dispatcher = dispatcher;
        _queueName = queueName;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken ct = default)
    {
        if (IsRunning)
        {
            return;
        }

        _channel = await _connection.CreateChannelAsync(ct);
        _consumeCts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        _consumeTask = _channel.StartConsumingAsync(_queueName, HandleMessageAsync, _consumeCts.Token);
        IsRunning = true;

        _logger.LogInformation("InMemoryConsumer started on queue '{QueueName}'", _queueName);
    }

    public async Task StopAsync(CancellationToken ct = default)
    {
        if (!IsRunning)
        {
            return;
        }

        IsRunning = false;

        if (_consumeCts is not null)
        {
            await _consumeCts.CancelAsync();
        }

        if (_consumeTask is not null)
        {
            try
            {
                await _consumeTask.WaitAsync(ct);
            }
            catch (OperationCanceledException)
            {
                // Expected during graceful shutdown
            }
        }

        _logger.LogInformation("InMemoryConsumer stopped on queue '{QueueName}'", _queueName);
    }

    private async Task<ProcessResult> HandleMessageAsync(ReceivedMessage message)
    {
        try
        {
            // RPC messages have a ReplyTo set — dispatch through RPC path
            if (!string.IsNullOrEmpty(message.ReplyTo))
            {
                return await HandleRpcMessageAsync(message);
            }

            // Standard fire-and-forget message
            var result = await _dispatcher.DispatchStandardAsync(message, CancellationToken.None);

            if (result == ProcessResult.Success)
            {
                await AcknowledgeAsync(message);
                return ProcessResult.Success;
            }

            await NegativeAcknowledgeAsync(message);
            return result;
        }
        catch (Exception ex)
        {
            // Consumer Resilience — never let processor exceptions crash the consume loop
            _logger.LogError(ex, "Error processing message {MessageId} on queue '{QueueName}'",
                message.MessageId, _queueName);
            await NegativeAcknowledgeAsync(message);
            return ProcessResult.Failure;
        }
    }

    private async Task<ProcessResult> HandleRpcMessageAsync(ReceivedMessage message)
    {
        var (result, responseBytes) = await _dispatcher.DispatchRpcAsync(message, CancellationToken.None);

        if (result == ProcessResult.Success && responseBytes is not null)
        {
            var responseEnvelope = new MessageEnvelope
            {
                Body = responseBytes,
                MessageId = Guid.NewGuid().ToString(),
                MessageType = "RpcResponse",
                CorrelationId = message.MessageId,
                RoutingKey = message.ReplyTo,
                Timestamp = DateTimeOffset.UtcNow,
                Headers = new Dictionary<string, object?>()
            };

            var replyChannel = await _connection.CreateChannelAsync();
            await replyChannel.PublishAsync(responseEnvelope);
            await replyChannel.DisposeAsync();

            await AcknowledgeAsync(message);
            return ProcessResult.Success;
        }

        await NegativeAcknowledgeAsync(message);
        return result;
    }

    private Task AcknowledgeAsync(ReceivedMessage message)
    {
        return _channel?.AcknowledgeAsync(message.DeliveryTag) ?? Task.CompletedTask;
    }

    private Task NegativeAcknowledgeAsync(ReceivedMessage message)
    {
        return _channel?.NegativeAcknowledgeAsync(message.DeliveryTag, requeue: false) ?? Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        if (IsRunning)
        {
            await StopAsync();
        }

        if (_channel is not null)
        {
            await _channel.DisposeAsync();
            _channel = null;
        }

        _consumeCts?.Dispose();
        _consumeCts = null;
    }
}
