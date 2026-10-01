# Requirements Document

## Introduction

MqCSFramework is an open-source, lightweight RabbitMQ client/server framework for .NET 10. It provides a simple way to send and consume messages using two patterns: Standard (fire-and-forget) and RPC (request-reply). The framework uses processor contract interfaces to create a compile-time link between sender and consumer.

**Project name:** MqCSFramework
**License:** MIT
**Target:** .NET 10, C# 14
**Broker:** RabbitMQ (via RabbitMQ.Client 7.x)

## Glossary

| Term | Definition |
|------|------------|
| Standard Messaging | Fire-and-forget — sender publishes, doesn't wait for response |
| RPC Messaging | Request-reply — sender publishes and awaits a typed response |
| Processor | A handler class that processes a specific message type |
| Processor Contract Interface | A shared interface (e.g., `IMyProcessor : IMessageProcessor<MyMessage>`) referenced by both sender and consumer |
| ACK/NACK | Acknowledge/Negative-Acknowledge — broker confirmation |

## Requirements

### Requirement 1: Standard Messaging (Fire-and-Forget)

**User Story:** As a developer, I want to publish messages to a RabbitMQ queue without waiting for a response.

#### Acceptance Criteria

1. The standard sender interface is `IStandardSender`
2. The send method signature is `SendAsync<TProcessor>(TMessage message, ...)` where `TProcessor : IMessageProcessor<TMessage>` — the message type is checked at compile time
3. The processor interface's AssemblyQualifiedName is set as the `mq-processor-type` message header
4. An additional header identifies the message pattern as "standard" (so the consumer knows which base interface to cast to)
5. Messages include metadata: message ID (GUID), timestamp, correlation ID
6. The message is serialized and published to the configured exchange/routing key

### Requirement 2: RPC Messaging (Request-Reply)

**User Story:** As a developer, I want to send a message and await a typed response from the consumer.

#### Acceptance Criteria

1. The RPC sender interface is `IRpcSender`
2. The send method signature is `SendAsync<TProcessor, TResponse>(TRequest request, ...)` where `TProcessor : IRpcProcessor<TRequest, TResponse>` and `TResponse : RpcResponse` — request and response types are checked at compile time
3. The processor interface's AssemblyQualifiedName is set as the `mq-processor-type` message header
4. An additional header identifies the message pattern as "rpc" (so the consumer knows which base interface to cast to)
5. The RPC sender declares an exclusive reply queue named `{routingKey}.reply.{GUID}` (unique per sender instance)
6. The consumer processes the request and publishes the serialized `TResponse` directly to the reply queue specified in the ReplyTo property (no wrapping envelope)
7. Responses are correlated by correlation ID
8. Timeout produces `RpcTimeoutException` (a client-side concern — the sender gave up waiting)
9. Every RPC response type MUST extend the abstract `RpcResponse` base, which carries the operation outcome: an optional `RpcError` (`Code` + `Message`) and a derived `Success` (true when `Error` is null). The processor expresses business success or failure by setting `Error` on the response it returns — it does not throw for business outcomes
10. Unexpected exceptions thrown by an RPC processor are NOT sent back to the sender. They follow the same failure path as standard messages (NACK, retry, dead-letter per Requirement 9). The sender observes only a timeout in this case

### Requirement 3: Consumer and Processor Resolution

**User Story:** As a developer, I want the consumer to automatically resolve the correct processor from DI based on the message header.

#### Acceptance Criteria

1. Processors inherit from abstract base classes: `StandardProcessor<TMessage>` (for standard) or `RpcProcessor<TRequest, TResponse>` (for RPC)
2. Processor implementations are registered by the developer in DI, recommended as Scoped: `services.AddScoped<IMyProcessor, MyProcessorImpl>()`. Each message is processed within its own DI scope, so scoped processors (and their scoped dependencies like DbContext) get a fresh instance per message. Singleton or Transient registration is also supported depending on the use case.
3. The generic interfaces extend non-generic base interfaces (`IMessageProcessor`, `IRpcProcessor`) with raw byte methods (`ProcessRawAsync`, `ProcessRawRpcAsync`) — the abstract base classes implement deserialization internally
4. When a message arrives, the consumer reads the `mq-processor-type` header to get the processor interface name
5. The consumer reads the `mq-pattern` header ("standard" or "rpc") to determine which non-generic interface to cast to
6. The consumer creates a new DI scope per message and resolves the processor from that scope using `Type.GetType(header)` + `scope.ServiceProvider.GetService(type)`. This ensures scoped dependencies (e.g. DbContext) are isolated per message.
7. For standard: casts to `IMessageProcessor` (non-generic) and calls `ProcessRawAsync(body, context, ct)`
8. For RPC: casts to `IRpcProcessor` (non-generic) and calls `ProcessRawRpcAsync(body, context, ct)`
9. Messages without the `mq-processor-type` header are rejected (NACK'd)
10. Messages with an unregistered processor type are NACK'd and logged

### Requirement 4: Independent Connections

**User Story:** As a developer, I want each sender/consumer to have its own RabbitMQ connection so I can connect to different brokers from one service.

#### Acceptance Criteria

1. Each sender and consumer carries its own connection configuration (host, credentials, virtual host, SSL)
2. A single service can have senders connected to different RabbitMQ clusters
3. One connection failing does not affect others
4. Connections auto-reconnect on failure

### Requirement 5: DI Integration

**User Story:** As a developer, I want to configure senders and consumers via a fluent builder pattern.

#### Acceptance Criteria

1. Extension method `AddMqCSFramework(mq => { ... })` registers framework services with manual configuration
2. Extension method `AddMqCSFramework(IConfiguration)` auto-binds from config section `"MqCSFramework"`
3. Extension method `AddMqCSFramework(IConfiguration, "SectionName")` auto-binds from the specified config section
4. `mq.AddSender("name", opts => { ... })` registers a keyed `IStandardSender`
5. `mq.AddRpcSender("name", opts => { ... })` registers a keyed `IRpcSender`
6. `mq.AddConsumer("name", opts => { ... })` registers a consumer
7. `mq.BindConfiguration(IConfigurationSection)` auto-registers all senders/rpcSenders/consumers from config sections
8. Senders are injected via `[FromKeyedServices("name")] IStandardSender sender`
9. Configuration binds from `IConfiguration` (appsettings.json)

### Requirement 6: Consumer Hosting

**User Story:** As a developer, I want consumers to run as a hosted service within a .NET Generic Host application.

#### Acceptance Criteria

1. A `BackgroundService` starts and manages all registered consumers
2. Graceful shutdown via `CancellationToken`
3. Multiple consumers can run in a single host

### Requirement 7: Serialization

**User Story:** As a developer, I want messages serialized as JSON by default.

#### Acceptance Criteria

1. System.Text.Json is used for serialization/deserialization
2. Serialization errors produce clear exceptions

### Requirement 8: Logging

**User Story:** As a developer, I want structured logging for all messaging operations.

#### Acceptance Criteria

1. All log messages use `ILogger` via Serilog
2. Sample projects configure Serilog with file sink writing to `C:\Logging\` directory
3. The consumer wraps each message's processing in a logging scope with `CorrelationId` — every log entry during that message's processing automatically includes the correlation ID (via `ILogger.BeginScope`)
4. Message body logging is controlled via Serilog log level configuration (Debug level) — use per-namespace overrides to enable/disable
5. Sensitive field masking (configurable list of field names replaced with `***MASKED***`)
6. Configuration (connection details, queue names, etc.) is read from `appsettings.json` — not hardcoded

### Requirement 9: Error Handling

**User Story:** As a developer, I want clear error handling so failures don't crash the consumer.

#### Acceptance Criteria

1. Processor exceptions do not crash the consumer loop
2. Failed messages are NACK'd
3. Dead-letter support: messages exceeding a retry limit are routed to an error queue
4. This path applies to both standard and RPC processors: an unexpected exception from an RPC processor is NACK'd/retried/dead-lettered exactly like a standard message. No error response is published to the reply queue — business failures must instead be returned as an `RpcError` on the response (see Requirement 2)

## Non-Functional Requirements

- Target: .NET 10 (LTS), C# 14
- All dependencies publicly available on NuGet.org
- MIT license
- Nullable reference types enabled
- Async-first APIs

## Resolved Decisions

| # | Decision |
|---|----------|
| 1 | RabbitMQ only — no transport abstraction layer for now |
| 2 | Processors recommended as Scoped DI registrations (one instance per message scope). Singleton and Transient also supported. |
| 3 | Sender always specifies processor contract interface (no fallback routing) |
| 4 | Each sender/consumer has its own independent connection |
| 5 | Three packages: `MqCSFramework` (core/shared), `MqCSFramework.Sender`, `MqCSFramework.Consumer` |
