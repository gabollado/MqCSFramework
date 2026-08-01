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
