# MqCSFramework

A .NET 10 framework for building client/server communication through centralized message queues using RabbitMQ.

MqCSFramework simplifies service-to-service messaging by providing a type-safe, compile-time-checked API for sending and consuming messages through a central bus. Services communicate via queues rather than direct HTTP calls — decoupling producers from consumers, enabling independent scaling, and providing built-in resilience through message persistence and retry mechanisms.

## Features

- **Two messaging patterns**: Standard (fire-and-forget) and RPC (request-reply)
- **Compile-time safety** — generic constraints ensure message types match processor expectations
- **Zero-reflection dispatch** — processors resolved from DI, called via non-generic interface
- **Independent connections** — each sender/consumer has its own RabbitMQ connection
- **One-line configuration** — `builder.Services.AddMqCSFramework(builder.Configuration)`
- **Automatic retry + dead-letter** — configurable retry count with error queue routing
- **Structured logging** — Serilog with configurable sensitive field masking

## Quick Start

```csharp
// Define contracts (shared project)
public record OrderMessage(Guid OrderId, string Customer, decimal Amount);
public interface IOrderProcessor : IMessageProcessor<OrderMessage>;

// Implement processor (consumer project)
public class OrderProcessor : StandardProcessor<OrderMessage>, IOrderProcessor
{
    public override Task ProcessAsync(OrderMessage msg, MessageContext ctx, CancellationToken ct)
    {
        Console.WriteLine($"Processing order {msg.OrderId}");
        return Task.CompletedTask;
    }
}

// Consumer setup
builder.Services.AddSingleton<IOrderProcessor, OrderProcessor>();
builder.Services.AddMqCSFramework(builder.Configuration);

// Sender setup
builder.Services.AddMqCSFramework(builder.Configuration);
var sender = app.Services.GetRequiredKeyedService<IStandardSender>("orders");
await sender.SendAsync<IOrderProcessor, OrderMessage>(new OrderMessage(...));
```

## Documentation

| Document | Description |
|----------|-------------|
| [Overview](docs/overview.md) | Architecture, use cases, how it works |
| [Quick Start](docs/quickstart.md) | Get running in 5 minutes |
| [Configuration](docs/configuration.md) | All options and appsettings.json reference |
| [API Reference](docs/api-reference.md) | Interfaces, classes, methods |
| [Samples](docs/samples.md) | Running included samples + build your own |

## Requirements

- .NET 10 SDK
- RabbitMQ instance

## License

MIT — see [LICENSE](LICENSE) for details.
