# Overview

## What is MqCSFramework?

MqCSFramework is an open-source .NET 10 framework that simplifies RabbitMQ messaging. It provides a type-safe, DI-native way to send and consume messages using two patterns:

- **Standard messaging** (fire-and-forget): publish a message to a queue without waiting for a response
- **RPC messaging** (request-reply): publish a request and await a typed response from the consumer

## Key Features

- **Compile-time type safety** — the sender specifies the processor interface as a generic parameter, so the message type is enforced by the compiler
- **Zero-reflection dispatch** — the consumer resolves processors from DI and calls them via non-generic base interfaces (no runtime reflection)
- **Independent connections** — each sender and consumer manages its own RabbitMQ connection, allowing one service to connect to multiple brokers
- **Configuration from appsettings.json** — one-line setup with `AddMqCSFramework(configuration)`
- **Automatic retry and dead-letter** — failed messages are retried up to a configurable limit, then routed to an error queue
- **Structured logging** — Serilog integration with configurable message body masking for sensitive fields
- **Simple processor model** — inherit from `StandardProcessor<T>` or `RpcProcessor<TReq, TRes>`, implement one method

## Benefits

### Independent Development

Client and server teams can work independently. Once the contracts (processor interface + message types) are defined, the sender team and consumer team can develop, test, and deploy without waiting on each other. The queue acts as the integration point — no need for both sides to be running simultaneously during development.

### Simple Routing

Every communication flows through a single routing point (the queue). Adding a new service or functionality doesn't introduce networking complexity — you just point it at the right queue. No service discovery, no load balancer configuration, no API gateway rules.

### Clear Responsibility Boundaries

When something goes wrong, you can inspect the message sitting in the queue. If the message is correct, the sender did its job — the issue is on the consumer side. This makes debugging distributed systems straightforward: check the queue, identify which side owns the problem.

### Technology and Version Independence

The wire format is standard (RabbitMQ + JSON). Any technology that can produce or consume AMQP messages with JSON bodies is automatically compatible. Versioning is simplified because JSON is forward-compatible — older consumers can ignore new fields they don't recognize, and newer consumers can handle messages with missing optional fields.

### Infrastructure Flexibility

RabbitMQ acts as a message bus, giving you infrastructure-level routing without code changes. You can redirect messages between environments, add multiple consumers to a queue for competing-consumer patterns, or fan out messages to multiple queues — all by configuring exchanges and bindings in RabbitMQ, without touching application code.

### Automatic Scalability

The sender is completely decoupled from the number of consumers. To scale processing, just start more consumer instances — RabbitMQ distributes messages across them automatically (competing consumers). No code changes, no sender reconfiguration, no load balancer updates. Scaling is an infrastructure operation, not a development task.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Shared Contracts (referenced by sender + consumer)         │
│  ┌─────────────────────┐  ┌──────────────────────────────┐ │
│  │ IOrderProcessor     │  │ IStockProcessor              │ │
│  │ : IMessageProcessor │  │ : IRpcProcessor<Req, Res>    │ │
│  │   <OrderMessage>    │  │                              │ │
│  └─────────────────────┘  └──────────────────────────────┘ │
│  ┌─────────────────────┐  ┌──────────────────────────────┐ │
│  │ OrderMessage        │  │ StockRequest / StockResponse │ │
│  └─────────────────────┘  └──────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘

┌──────────────────────┐        ┌──────────────────────────┐
│  Sender Service      │        │  Consumer Service        │
│                      │        │                          │
│  IStandardSender     │──────► │  RabbitMQ Queue          │
│  .SendAsync<         │  msg   │                          │
│    IOrderProcessor,  │        │  MqConsumer resolves     │
│    OrderMessage>     │        │  IOrderProcessor from DI │
│                      │        │  → calls ProcessAsync    │
│  IRpcSender          │        │                          │
│  .SendAsync<         │──req──►│  Resolves IStockProcessor│
│    IStockProcessor,  │        │  → calls ProcessAsync    │
│    StockResponse,    │◄─res───│  → returns response      │
│    StockRequest>     │        │                          │
└──────────────────────┘        └──────────────────────────┘
```

## Use Cases

### Microservice Communication
Decouple services by communicating through message queues. The sender doesn't need to know the consumer's address — it just publishes to a queue.

### Background Processing
Offload heavy or slow tasks (email sending, PDF generation, data imports) to a consumer service that processes them asynchronously.

### Request-Reply Between Services
When you need a synchronous-style response from another service but want the benefits of message-based communication (resilience, retries, load leveling).

### Multi-Tenant Processing
Each tenant can have its own queue with a dedicated consumer, while senders route messages by tenant configuration.

### Event-Driven Architecture
Publish domain events that multiple consumers can process independently (though this requires exchange/routing configuration beyond simple queues).

## How It Works

1. **Define contracts** — create processor interfaces and message records in a shared project
2. **Implement processors** — inherit from `StandardProcessor<T>` or `RpcProcessor<TReq, TRes>`
3. **Register in DI** — add processors as singletons
4. **Configure** — set up connection, queues, and senders in `appsettings.json`
5. **Send** — inject `IStandardSender` or `IRpcSender` and call `SendAsync`
6. **Consume** — the framework handles everything: connection, subscription, dispatch, ACK/NACK, retries

## Technology Stack

| Component | Technology |
|-----------|------------|
| Runtime | .NET 10 (LTS) |
| Language | C# 14 |
| Broker | RabbitMQ via RabbitMQ.Client 7.x |
| Serialization | System.Text.Json |
| DI | Microsoft.Extensions.DependencyInjection |
| Hosting | Microsoft.Extensions.Hosting (BackgroundService) |
| Logging | Microsoft.Extensions.Logging (Serilog recommended) |
| License | MIT |
