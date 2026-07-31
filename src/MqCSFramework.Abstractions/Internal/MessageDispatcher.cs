using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using MqCSFramework.Abstractions.Constants;
using MqCSFramework.Abstractions.Exceptions;
using MqCSFramework.Abstractions.Models;
using MqCSFramework.Abstractions.Processor;
using MqCSFramework.Abstractions.Serialization;

namespace MqCSFramework.Abstractions.Internal;

/// <summary>
/// Dispatches incoming messages to the correct processor by resolving it directly from DI
/// using the processor interface type name from the mq-processor-type header.
/// No routing dictionaries, no startup registration — just DI resolution + type cache.
/// </summary>
internal sealed class MessageDispatcher
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IMessageSerializer _serializer;
    private readonly ILogger<MessageDispatcher> _logger;

    // Cache: processor interface Type → TMessage (or TRequest) type
    private readonly ConcurrentDictionary<Type, Type> _messageTypeCache = new();
    // Cache: processor interface Type → TResponse type (for RPC)
    private readonly ConcurrentDictionary<Type, Type> _responseTypeCache = new();

    private static readonly Type StandardProcessorOpenGeneric = typeof(IMessageProcessor<>);
    private static readonly Type RpcProcessorOpenGeneric = typeof(IRpcProcessor<,>);

    public MessageDispatcher(
        IServiceProvider serviceProvider,
        IMessageSerializer serializer,
        ILogger<MessageDispatcher> logger)
    {
        _serviceProvider = serviceProvider;
        _serializer = serializer;
        _logger = logger;
    }

    /// <summary>
    /// Dispatches a standard (fire-and-forget) message to its processor.
    /// </summary>
    public async Task<ProcessResult> DispatchStandardAsync(ReceivedMessage message, CancellationToken ct)
    {
        var (processorType, processor) = ResolveProcessor(message);
        if (processor is null)
        {
            return ProcessResult.Failure;
        }

        try
        {
            var messageType = GetMessageType(processorType);
            var deserializedMessage = _serializer.Deserialize(message.Body, messageType);
            var context = BuildMessageContext(message, ct);

            // Invoke ProcessAsync via the generic interface
            var processMethod = typeof(IMessageProcessor<>)
                .MakeGenericType(messageType)
                .GetMethod(nameof(IMessageProcessor<object>.ProcessAsync))!;

            var task = (Task)processMethod.Invoke(processor, [deserializedMessage, context, ct])!;
            await task.ConfigureAwait(false);

            return ProcessResult.Success;
        }
        catch (UnknownMessageTypeException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing message. ProcessorType: {ProcessorType}, MessageId: {MessageId}",
                processorType.FullName, message.MessageId);
            return ProcessResult.Failure;
        }
    }

    /// <summary>
    /// Dispatches an RPC message to its processor and returns the serialized response.
    /// </summary>
    public async Task<(ProcessResult Result, byte[]? Response)> DispatchRpcAsync(ReceivedMessage message, CancellationToken ct)
    {
        var (processorType, processor) = ResolveProcessor(message);
        if (processor is null)
        {
            return (ProcessResult.Failure, null);
        }

        try
        {
            var messageType = GetMessageType(processorType);
            var responseType = GetResponseType(processorType);
            var deserializedRequest = _serializer.Deserialize(message.Body, messageType);
            var context = BuildMessageContext(message, ct);

            // Invoke ProcessAsync via the generic interface
            var processorInterfaceType = typeof(IRpcProcessor<,>).MakeGenericType(messageType, responseType);
            var processMethod = processorInterfaceType.GetMethod(nameof(IRpcProcessor<object, object>.ProcessAsync))!;

            var task = (Task)processMethod.Invoke(processor, [deserializedRequest, context, ct])!;
            await task.ConfigureAwait(false);

            // Extract the result from Task<TResponse>
            var resultProperty = task.GetType().GetProperty("Result")!;
            var response = resultProperty.GetValue(task)!;

            // Serialize the response
            var serializeMethod = typeof(IMessageSerializer)
                .GetMethod(nameof(IMessageSerializer.Serialize))!
                .MakeGenericMethod(responseType);
            var responseBytes = (byte[])serializeMethod.Invoke(_serializer, [response])!;

            return (ProcessResult.Success, responseBytes);
        }
        catch (UnknownMessageTypeException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing RPC message. ProcessorType: {ProcessorType}, MessageId: {MessageId}",
                processorType.FullName, message.MessageId);
            return (ProcessResult.Failure, null);
        }
    }

    private (Type processorType, object? processor) ResolveProcessor(ReceivedMessage message)
    {
        var headerValue = GetProcessorTypeHeader(message);
        if (headerValue is null)
        {
            _logger.LogWarning("Message {MessageId} is missing the '{Header}' header",
                message.MessageId, MessageHeaders.ProcessorType);
            throw new UnknownMessageTypeException($"Missing header: {MessageHeaders.ProcessorType}");
        }

        var processorType = Type.GetType(headerValue);
        if (processorType is null)
        {
            _logger.LogWarning("Cannot resolve processor type '{ProcessorTypeName}' for message {MessageId}",
                headerValue, message.MessageId);
            throw new UnknownMessageTypeException(headerValue);
        }

        var processor = _serviceProvider.GetService(processorType);
        if (processor is null)
        {
            _logger.LogError("Processor '{ProcessorType}' is not registered in DI for message {MessageId}",
                processorType.FullName, message.MessageId);
            throw new UnknownMessageTypeException(processorType.FullName ?? headerValue);
        }

        return (processorType, processor);
    }

    private Type GetMessageType(Type processorInterfaceType)
    {
        return _messageTypeCache.GetOrAdd(processorInterfaceType, static type =>
        {
            // Check IMessageProcessor<TMessage>
            var standardInterface = FindInterface(type, StandardProcessorOpenGeneric);
            if (standardInterface is not null)
            {
                return standardInterface.GetGenericArguments()[0];
            }

            // Check IRpcProcessor<TRequest, TResponse>
            var rpcInterface = FindInterface(type, RpcProcessorOpenGeneric);
            if (rpcInterface is not null)
            {
                return rpcInterface.GetGenericArguments()[0]; // TRequest
            }

            throw new InvalidOperationException(
                $"Type '{type.FullName}' does not implement IMessageProcessor<T> or IRpcProcessor<TReq, TRes>.");
        });
    }

    private Type GetResponseType(Type processorInterfaceType)
    {
        return _responseTypeCache.GetOrAdd(processorInterfaceType, static type =>
        {
            var rpcInterface = FindInterface(type, RpcProcessorOpenGeneric);
            if (rpcInterface is not null)
            {
                return rpcInterface.GetGenericArguments()[1]; // TResponse
            }

            throw new InvalidOperationException(
                $"Type '{type.FullName}' does not implement IRpcProcessor<TRequest, TResponse>.");
        });
    }

    private static Type? FindInterface(Type type, Type openGenericInterface)
    {
        // If the type itself is a generic interface matching the open generic, use it directly
        if (type.IsInterface && type.IsGenericType && type.GetGenericTypeDefinition() == openGenericInterface)
        {
            return type;
        }

        // Search all interfaces implemented by the type
        foreach (var iface in type.GetInterfaces())
        {
            if (iface.IsGenericType && iface.GetGenericTypeDefinition() == openGenericInterface)
            {
                return iface;
            }
        }

        return null;
    }

    private static string? GetProcessorTypeHeader(ReceivedMessage message)
    {
        if (!message.Headers.TryGetValue(MessageHeaders.ProcessorType, out var value))
        {
            return null;
        }

        return value?.ToString();
    }

    private static MessageContext BuildMessageContext(ReceivedMessage message, CancellationToken ct)
    {
        return new MessageContext
        {
            MessageId = message.MessageId,
            CorrelationId = message.CorrelationId ?? message.MessageId,
            MessageType = message.MessageType,
            Timestamp = message.Timestamp,
            SenderIdentity = message.SenderIdentity,
            Headers = message.Headers,
            Redelivered = message.Redelivered,
            CancellationToken = ct
        };
    }
}
