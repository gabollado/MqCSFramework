# MqCSFramework

A lightweight, broker-agnostic message queue framework for .NET 10.

## Features

- **Broker-agnostic abstractions** — swap transports without changing application code
- **Two messaging patterns**: Standard (fire-and-forget) and RPC (request-reply)
- **Two transport implementations**: RabbitMQ and In-Memory (for testing)
- **Processor-linked routing** — compile-time safety between sender and consumer
- **Independent connections per sender/consumer** — connect to multiple brokers from one service
- **Built-in health checks** and OpenTelemetry tracing
- **Pluggable serialization** (System.Text.Json default)
- **Modern .NET 10** — async/await, nullable, keyed DI services

## Package Structure

| Package | Purpose |
|---------|---------|
| `MqCSFramework.Abstractions` | Interfaces, models, base classes (no broker dependency) |
| `MqCSFramework.RabbitMQ` | RabbitMQ transport implementation |
| `MqCSFramework.InMemory` | In-memory transport for testing/development |
| `MqCSFramework.Hosting` | BackgroundService, DI extensions, health checks |

## Quick Start

```csharp
// Sender service
builder.Services.AddMqCSFramework(mq =>
{
    mq.AddSender("orders", opts =>
    {
        builder.Configuration.GetSection("MqCSFramework:Senders:orders").Bind(opts);
    });
});

// Inject and use
public class OrderService([FromKeyedServices("orders")] IMessageSender sender)
{
    public async Task PlaceOrderAsync(OrderPlaced order)
    {
        await sender.SendAsync<OrderPlacedProcessor>(order);
    }
}
```

```csharp
// Consumer service
builder.Services.AddMqCSFramework(mq =>
{
    mq.AddConsumer("orders", opts =>
    {
        builder.Configuration.GetSection("MqCSFramework:Consumers:orders").Bind(opts);
    });
    mq.AddProcessor<OrderPlacedProcessor, OrderPlaced>();
});

// Processor implementation
public class OrderPlacedProcessor : IMessageProcessor<OrderPlaced>
{
    public async Task ProcessAsync(OrderPlaced message, MessageContext context, CancellationToken ct)
    {
        // Handle the order...
    }
}
```

## Requirements

- .NET 10 SDK
- RabbitMQ (for the RabbitMQ transport — not needed with InMemory transport)

## License

MIT — see [LICENSE](LICENSE) for details.
