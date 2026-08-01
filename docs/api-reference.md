# API Reference

## Public Interfaces

### IMessageProcessor (non-generic base)

```csharp
namespace MqCSFramework;

public interface IMessageProcessor
{
    Task ProcessRawAsync(ReadOnlyMemory<byte> body, MessageContext context, CancellationToken ct = default);
}
```

The consumer calls this method directly. You never implement this yourself — the abstract base class `StandardProcessor<T>` handles it.

---

### IMessageProcessor&lt;TMessage&gt;

```csharp
public interface IMessageProcessor<in TMessage> : IMessageProcessor where TMessage : class
{
    Task ProcessAsync(TMessage message, MessageContext context, CancellationToken ct = default);
}
```

Define your processor contract interface by inheriting from this:

```csharp
public interface IOrderProcessor : IMessageProcessor<OrderMessage>;
```

---

### IRpcProcessor (non-generic base)

```csharp
public interface IRpcProcessor
{
    Task<byte[]> ProcessRawRpcAsync(ReadOnlyMemory<byte> body, MessageContext context, CancellationToken ct = default);
}
```

The consumer calls this method directly for RPC messages.

---

### IRpcProcessor&lt;TRequest, TResponse&gt;

```csharp
public interface IRpcProcessor<in TRequest, TResponse> : IRpcProcessor
    where TRequest : class
    where TResponse : class
{
    Task<TResponse> ProcessAsync(TRequest request, MessageContext context, CancellationToken ct = default);
}
```

Define your RPC processor contract interface:

```csharp
public interface IStockProcessor : IRpcProcessor<StockRequest, StockResponse>;
```

---

### IStandardSender

```csharp
public interface IStandardSender
{
    Task<string> SendAsync<TProcessor, TMessage>(
        TMessage message,
        SendOptions? options = null,
        CancellationToken ct = default)
        where TProcessor : IMessageProcessor<TMessage>
        where TMessage : class;
}
```

**Returns:** The generated message ID (GUID string).

**Generic constraints** enforce compile-time safety — you cannot send a message type that doesn't match the processor's expectation.

---

### IRpcSender

```csharp
public interface IRpcSender
{
    Task<TResponse> SendAsync<TProcessor, TResponse, TRequest>(
        TRequest request,
        RpcOptions? options = null,
        CancellationToken ct = default)
        where TProcessor : IRpcProcessor<TRequest, TResponse>
        where TRequest : class
        where TResponse : class;
}
```

**Returns:** The deserialized response of type `TResponse`.

**Throws:**
- `RpcTimeoutException` — if no response within the configured timeout
- `RpcRemoteException` — if the processor threw an exception

---

## Abstract Base Classes

### StandardProcessor&lt;TMessage&gt;

```csharp
public abstract class StandardProcessor<TMessage> : IMessageProcessor<TMessage>
    where TMessage : class
{
    // Implemented by the base class — handles deserialization
    public Task ProcessRawAsync(ReadOnlyMemory<byte> body, MessageContext context, CancellationToken ct);

    // You implement this
    public abstract Task ProcessAsync(TMessage message, MessageContext context, CancellationToken ct = default);
}
```

**Usage:**

```csharp
public class OrderProcessor : StandardProcessor<OrderMessage>, IOrderProcessor
{
    public override Task ProcessAsync(OrderMessage message, MessageContext context, CancellationToken ct = default)
    {
        // Your business logic here
        return Task.CompletedTask;
    }
}
```

---

### RpcProcessor&lt;TRequest, TResponse&gt;

```csharp
public abstract class RpcProcessor<TRequest, TResponse> : IRpcProcessor<TRequest, TResponse>
    where TRequest : class
    where TResponse : class
{
    // Implemented by the base class — handles deserialization + response serialization
    public Task<byte[]> ProcessRawRpcAsync(ReadOnlyMemory<byte> body, MessageContext context, CancellationToken ct);

    // You implement this
    public abstract Task<TResponse> ProcessAsync(TRequest request, MessageContext context, CancellationToken ct = default);
}
```

**Usage:**

```csharp
public class StockProcessor : RpcProcessor<StockRequest, StockResponse>, IStockProcessor
{
    public override Task<StockResponse> ProcessAsync(StockRequest request, MessageContext context, CancellationToken ct = default)
    {
        var response = new StockResponse(Available: true, RemainingStock: 42);
        return Task.FromResult(response);
    }
}
```

---

## Models

### MessageContext

```csharp
public sealed record MessageContext
{
    public required string MessageId { get; init; }
    public required string CorrelationId { get; init; }
    public required DateTimeOffset Timestamp { get; init; }
    public required string Pattern { get; init; }          // "standard" or "rpc"
    public required IReadOnlyDictionary<string, string> Headers { get; init; }
}
```

Available to all processors — contains metadata about the received message.

---

### MqHeaders

```csharp
public static class MqHeaders
{
    public const string ProcessorType = "mq-processor-type";
    public const string Pattern = "mq-pattern";
    public const string RetryCount = "mq-retry-count";

    public const string PatternStandard = "standard";
    public const string PatternRpc = "rpc";
}
```

---

## Exceptions

### RpcTimeoutException

Thrown when an RPC call doesn't receive a response within the configured timeout.

| Property | Type | Description |
|----------|------|-------------|
| `CorrelationId` | string | The correlation ID of the timed-out request |
| `Timeout` | TimeSpan | The timeout that was exceeded |

---

### RpcRemoteException

Thrown when the remote processor threw an exception during RPC processing.

| Property | Type | Description |
|----------|------|-------------|
| `CorrelationId` | string | The correlation ID of the failed request |
| `RemoteExceptionType` | string | The type name of the original exception |

---

### MessageSerializationException

Thrown when message serialization or deserialization fails.

| Property | Type | Description |
|----------|------|-------------|
| `MessageId` | string? | The message ID (if available) |

---

## Extension Methods

### ServiceCollectionExtensions

| Method | Description |
|--------|-------------|
| `AddMqCSFramework(Action<MqBuilder>)` | Manual builder configuration |
| `AddMqCSFramework(IConfiguration)` | Auto-bind from `"MqCSFramework"` config section |
| `AddMqCSFramework(IConfiguration, string)` | Auto-bind from specified config section |

---

## MqBuilder Methods

| Method | Description |
|--------|-------------|
| `AddSender(string name, Action<StandardSenderOptions>)` | Register a keyed `IStandardSender` |
| `AddRpcSender(string name, Action<RpcSenderOptions>)` | Register a keyed `IRpcSender` |
| `AddConsumer(string name, Action<ConsumerOptions>)` | Register a consumer |
| `BindConfiguration(IConfigurationSection)` | Auto-register all senders/consumers from config |
