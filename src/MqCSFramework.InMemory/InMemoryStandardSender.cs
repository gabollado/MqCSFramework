using MqCSFramework.Abstractions.Configuration;
using MqCSFramework.Abstractions.Constants;
using MqCSFramework.Abstractions.Models;
using MqCSFramework.Abstractions.Processor;
using MqCSFramework.Abstractions.Sender;
using MqCSFramework.Abstractions.Serialization;
using MqCSFramework.Abstractions.Transport;

namespace MqCSFramework.InMemory;

/// <summary>
/// In-memory standard sender. Publishes messages through in-process channels
/// without network calls. Suitable for testing and local development.
/// </summary>
public sealed class InMemoryStandardSender : IMessageSender, IAsyncDisposable
{
    private readonly InMemoryTransportConnection _connection;
    private readonly IMessageSerializer _serializer;
    private ITransportChannel? _channel;

    public InMemoryStandardSender(InMemoryTransportConnection connection, IMessageSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(serializer);

        _connection = connection;
        _serializer = serializer;
    }

    /// <summary>
    /// Send a message targeting a specific processor contract interface.
    /// The interface's AssemblyQualifiedName is set as the mq-processor-type header.
    /// </summary>
    public async Task<string> SendAsync<TProcessor>(
        object message,
        SendOptions? options = null,
        CancellationToken ct = default)
        where TProcessor : class
    {
        ArgumentNullException.ThrowIfNull(message);

        var body = SerializeMessage(message);
        var messageId = Guid.NewGuid().ToString();
        var timestamp = DateTimeOffset.UtcNow;

        var headers = BuildHeaders(options);
        headers[MessageHeaders.ProcessorType] = typeof(TProcessor).AssemblyQualifiedName;

        var envelope = new MessageEnvelope
        {
            Body = body,
            MessageId = messageId,
            MessageType = message.GetType().FullName ?? message.GetType().Name,
            CorrelationId = options?.CorrelationId,
            Exchange = options?.Exchange,
            RoutingKey = options?.RoutingKey,
            Persistent = options?.Persistent ?? true,
            Timestamp = timestamp,
            SenderIdentity = options?.SenderIdentity,
            Headers = headers,
            ContentType = _serializer.ContentType
        };

        var channel = await GetChannelAsync(ct);
        await channel.PublishAsync(envelope, ct);

        return messageId;
    }

    private byte[] SerializeMessage(object message)
    {
        var method = typeof(IMessageSerializer)
            .GetMethod(nameof(IMessageSerializer.Serialize))!
            .MakeGenericMethod(message.GetType());

        return (byte[])method.Invoke(_serializer, [message])!;
    }

    private static Dictionary<string, object?> BuildHeaders(SendOptions? options)
    {
        var headers = new Dictionary<string, object?>();

        if (options?.Headers is not null)
        {
            foreach (var kvp in options.Headers)
            {
                headers[kvp.Key] = kvp.Value;
            }
        }

        if (!string.IsNullOrEmpty(options?.CorrelationId))
        {
            headers[MessageHeaders.CorrelationId] = options.CorrelationId;
        }

        if (!string.IsNullOrEmpty(options?.SenderIdentity))
        {
            headers[MessageHeaders.SenderIdentity] = options.SenderIdentity;
        }

        return headers;
    }

    private async Task<ITransportChannel> GetChannelAsync(CancellationToken ct)
    {
        if (_channel is not null)
        {
            return _channel;
        }

        _channel = await _connection.CreateChannelAsync(ct);
        return _channel;
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel is not null)
        {
            await _channel.DisposeAsync();
            _channel = null;
        }
    }
}
