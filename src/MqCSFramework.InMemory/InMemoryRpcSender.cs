using System.Collections.Concurrent;
using MqCSFramework.Abstractions.Configuration;
using MqCSFramework.Abstractions.Constants;
using MqCSFramework.Abstractions.Exceptions;
using MqCSFramework.Abstractions.Models;
using MqCSFramework.Abstractions.Sender;
using MqCSFramework.Abstractions.Serialization;
using MqCSFramework.Abstractions.Transport;

namespace MqCSFramework.InMemory;

/// <summary>
/// In-memory RPC sender. Uses TaskCompletionSource for response correlation.
/// Owns its own InMemoryTransportConnection and sets up a reply channel
/// to receive responses matched by CorrelationId.
/// </summary>
public sealed class InMemoryRpcSender : IRpcSender, IAsyncDisposable
{
    private readonly InMemoryTransportConnection _connection;
    private readonly IMessageSerializer _serializer;
    private readonly ConcurrentDictionary<string, TaskCompletionSource<byte[]>> _pendingRequests = new();
    private readonly string _replyQueueName = $"rpc-reply-{Guid.NewGuid():N}";
    private readonly TimeSpan _defaultTimeout;

    private ITransportChannel? _publishChannel;
    private ITransportChannel? _replyChannel;
    private Task? _replyConsumerTask;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private bool _initialized;

    public InMemoryRpcSender(
        InMemoryTransportConnection connection,
        IMessageSerializer serializer,
        TimeSpan? defaultTimeout = null)
    {
        _connection = connection;
        _serializer = serializer;
        _defaultTimeout = defaultTimeout ?? TimeSpan.FromSeconds(30);
    }

    /// <summary>
    /// Send an RPC request targeting a specific processor contract interface.
    /// TProcessor is the processor contract interface (e.g., ICheckStockProcessor).
    /// TResponse is the expected response type.
    /// </summary>
    public async Task<TResponse> SendAsync<TProcessor, TResponse>(
        object request,
        RpcOptions? options = null,
        CancellationToken ct = default)
        where TProcessor : class
        where TResponse : class
    {
        ArgumentNullException.ThrowIfNull(request);

        await EnsureInitializedAsync(ct);

        var messageId = Guid.NewGuid().ToString("N");
        var correlationId = options?.CorrelationId ?? messageId;
        var timeout = options?.Timeout ?? _defaultTimeout;

        var body = SerializeMessage(request);

        var headers = new Dictionary<string, object?>();
        if (options?.Headers is not null)
        {
            foreach (var kvp in options.Headers)
                headers[kvp.Key] = kvp.Value;
        }
        headers[MessageHeaders.ProcessorType] = typeof(TProcessor).AssemblyQualifiedName;

        var envelope = new MessageEnvelope
        {
            Body = body,
            MessageId = messageId,
            MessageType = request.GetType().FullName ?? request.GetType().Name,
            CorrelationId = correlationId,
            ReplyTo = _replyQueueName,
            RoutingKey = options?.RoutingKey,
            SenderIdentity = options?.SenderIdentity,
            Headers = headers,
            ContentType = _serializer.ContentType
        };

        return await PublishAndAwaitResponseAsync<TResponse>(envelope, timeout, ct);
    }

    private async Task<TResponse> PublishAndAwaitResponseAsync<TResponse>(
        MessageEnvelope envelope,
        TimeSpan timeout,
        CancellationToken ct)
        where TResponse : class
    {
        var tcs = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingRequests[envelope.MessageId] = tcs;

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(timeout);

        using var registration = timeoutCts.Token.Register(() =>
        {
            if (_pendingRequests.TryRemove(envelope.MessageId, out var pendingTcs))
            {
                pendingTcs.TrySetException(new RpcTimeoutException(
                    envelope.CorrelationId ?? envelope.MessageId,
                    envelope.MessageId,
                    timeout));
            }
        });

        await _publishChannel!.PublishAsync(envelope, ct);

        var responseBytes = await tcs.Task;

        // Check if the response is an error
        try
        {
            var errorResponse = _serializer.Deserialize<RpcErrorResponse>(responseBytes);
            if (errorResponse.IsError)
            {
                throw new RpcRemoteException(
                    errorResponse.ErrorCode,
                    errorResponse.ErrorMessage,
                    errorResponse.StackTrace);
            }
        }
        catch (RpcRemoteException)
        {
            throw;
        }
        catch
        {
            // Not an error response — proceed with normal deserialization
        }

        return _serializer.Deserialize<TResponse>(responseBytes);
    }

    private async Task EnsureInitializedAsync(CancellationToken ct)
    {
        if (_initialized)
            return;

        await _initLock.WaitAsync(ct);
        try
        {
            if (_initialized)
                return;

            _publishChannel = await _connection.CreateChannelAsync(ct);
            _replyChannel = await _connection.CreateChannelAsync(ct);

            _replyConsumerTask = _replyChannel.StartConsumingAsync(
                _replyQueueName,
                HandleReplyAsync,
                CancellationToken.None);

            _initialized = true;
        }
        finally
        {
            _initLock.Release();
        }
    }

    private Task<ProcessResult> HandleReplyAsync(ReceivedMessage message)
    {
        var correlationKey = message.CorrelationId ?? message.MessageId;

        if (_pendingRequests.TryRemove(correlationKey, out var tcs))
        {
            tcs.TrySetResult(message.Body);
        }

        return Task.FromResult(ProcessResult.Success);
    }

    private byte[] SerializeMessage(object message)
    {
        var method = typeof(IMessageSerializer)
            .GetMethod(nameof(IMessageSerializer.Serialize))!
            .MakeGenericMethod(message.GetType());

        return (byte[])method.Invoke(_serializer, [message])!;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var kvp in _pendingRequests)
        {
            if (_pendingRequests.TryRemove(kvp.Key, out var tcs))
            {
                tcs.TrySetCanceled();
            }
        }

        if (_replyChannel is not null)
            await _replyChannel.DisposeAsync();

        if (_publishChannel is not null)
            await _publishChannel.DisposeAsync();

        _initLock.Dispose();
    }
}
