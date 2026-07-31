# Requirements Document

## Introduction

An open-source, lightweight message queue framework for .NET 10 that provides a clean abstraction over message brokers. The framework ships with two transport implementations (RabbitMQ and In-Memory) and supports two messaging patterns: Standard (fire-and-forget) and RPC (request-reply), with shared infrastructure for common concerns.

**Project name:** MqCSFramework  
**License:** MIT (no warranty, use at your own risk, do whatever you want with it)

## Glossary

| Term | Definition |
|------|------------|
| Standard Messaging | Fire-and-forget pattern — sender publishes a message without waiting for a response |
| RPC Messaging | Request-reply pattern — sender publishes a message and awaits a typed response |
| Transport | The broker-specific implementation layer that handles actual message delivery |
| Processor | A handler class that processes a specific message type received by a consumer |
| ACK/NACK | Acknowledge/Negative-Acknowledge — broker confirmation that a message was processed or rejected |
| Confirm-Select | Broker feature that confirms message was received by the broker after publish |
| Dead-Letter Queue | A queue where messages are sent when they cannot be delivered or processed |

## Requirements

## Goals

- Simple, clean API that's easy to understand and use
- Broker-agnostic abstractions with RabbitMQ as the first concrete implementation
- Clear separation between Standard and RPC patterns while sharing common infrastructure
- Modern .NET 10 / C# 14 patterns (nullable, async/await, DI-native)
- Zero proprietary dependencies — all public NuGet packages only
- Production-ready: resilient connections, proper error handling, graceful shutdown

## Non-Goals

- Management UI or admin API
- Multi-broker routing (one broker per deployment)
- Saga/workflow orchestration
- Message persistence/outbox patterns (could be added later as separate package)

## Requirements

### Requirement 1: Transport Abstraction Layer

**User Story:** As a developer, I want broker-agnostic interfaces for message transport operations, so that I can swap messaging backends without changing application code.

#### Acceptance Criteria

1. Transport interfaces exist that do not reference any specific broker library
2. A RabbitMQ implementation package implements all transport interfaces
3. An In-Memory implementation package implements all transport interfaces
4. Swapping the transport requires only changing DI registration, not application code

### Requirement 2: Standard Messaging (Fire-and-Forget)

**User Story:** As a developer, I want to publish messages to a queue without waiting for a response, so that I can decouple services with asynchronous communication.

#### Acceptance Criteria

1. A sender can publish a serialized message with routing information
2. Messages include metadata: correlation ID, message ID, timestamp, message type, sender identity
3. The publisher receives confirmation that the message was accepted by the broker (when confirm-select is enabled)
4. A consumer can subscribe to a queue and receive messages
5. Messages are deserialized and dispatched to the appropriate processor based on message type
6. Messages are acknowledged (ACK) after successful processing
7. Messages are negatively acknowledged (NACK) on processing failure, with configurable requeue behavior

### Requirement 3: RPC Messaging (Request-Reply)

**User Story:** As a developer, I want to send a message and await a typed response from a remote consumer, so that I can implement synchronous-style communication over asynchronous messaging.

#### Acceptance Criteria

1. A sender can publish a message and await a typed response with a configurable timeout
2. Correlation between request and response is handled via message ID or correlation ID
3. The consumer processes the message and publishes the response back to the reply queue
4. Timeout produces a clear exception
5. Error responses from the consumer are propagated as exceptions to the sender
6. The RPC sender tracks pending requests and matches responses asynchronously

### Requirement 4: Message Serialization

**User Story:** As a developer, I want pluggable message serialization, so that I can choose the best format for my use case (JSON, MessagePack, Protobuf, etc.).

#### Acceptance Criteria

1. System.Text.Json is the default serializer
2. A serializer interface allows replacing with any other serializer
3. Serialization errors produce clear exceptions with context about what failed

### Requirement 5: Dependency Injection Integration

**User Story:** As a developer, I want the framework to integrate with Microsoft.Extensions.DependencyInjection, so that I can configure and use it following standard .NET patterns.

#### Acceptance Criteria

1. Extension methods register all required services
2. Senders are injectable via interfaces
3. Consumer processors are registered via DI and resolved per message
4. Configuration is bound from IConfiguration (appsettings.json sections)
5. Multiple sender/consumer configurations can coexist (named/keyed instances)

### Requirement 6: Consumer Hosting

**User Story:** As a developer, I want a hosted service for running consumers within a .NET Generic Host application, so that consumer lifecycle is managed automatically.

#### Acceptance Criteria

1. A BackgroundService implementation starts and manages consumer connections
2. Graceful shutdown is supported via CancellationToken
3. Multiple consumers (different queues) can run in a single host
4. Consumer startup logs connection details (without secrets)

### Requirement 7: Connection Resilience

**User Story:** As a developer, I want the framework to handle connection failures gracefully, so that my application recovers without manual intervention.

#### Acceptance Criteria

1. Automatic reconnection on connection loss (with configurable retry policy)
2. Sender resets its connection state after a publish failure and retries
3. Consumer reconnects and resumes listening after a broker restart
4. Connection failures are logged with appropriate severity

### Requirement 8: Message Processor Routing

**User Story:** As a developer, I want incoming messages to be routed to the correct processor, so that I can handle different message types on the same queue with compile-time safety.

#### Acceptance Criteria

1. Each processor declares which message type it handles
2. The consumer dispatches to the matching processor
3. Unknown message types are logged and NACK'd (not silently dropped)
4. Multiple processors can be registered for different message types on the same queue
5. Senders can specify the target processor type as a generic parameter, creating a compile-time link between sender and consumer
6. The processor type is propagated as a message header for routing on the consumer side
7. Both concrete processor types and shared contract interfaces are supported as the generic parameter
8. Fallback routing by message type name is supported for messages sent without a processor type reference

### Requirement 9: Logging and Observability

**User Story:** As a developer, I want structured logging for all framework operations, so that I can diagnose issues and monitor behavior in production.

#### Acceptance Criteria

1. All log messages use ILogger from Microsoft.Extensions.Logging
2. Correlation IDs are carried through the processing pipeline
3. Configurable log levels for message-level logging (to reduce noise in high-throughput scenarios)
4. Option to log or suppress message bodies
5. Sensitive field masking is configurable (list of field names whose values are replaced in logs)

### Requirement 10: Error Handling

**User Story:** As a developer, I want clear error handling and propagation, so that failures are visible and recoverable without crashing the consumer.

#### Acceptance Criteria

1. Processing exceptions do not crash the consumer
2. Unhandled exceptions in processors result in NACK with configurable requeue
3. Dead-letter / error queue support: messages that fail repeatedly can be routed to an error queue (configurable retry limit)

### Requirement 11: Health Checks

**User Story:** As a developer, I want health check endpoints for broker connectivity, so that I can monitor the system with standard ASP.NET Core health monitoring.

#### Acceptance Criteria

1. A health check verifies that the broker connection is alive and responsive
2. Health check reports degraded state on connection issues (not immediately unhealthy)
3. Health checks are registered via a simple extension method
4. Both sender and consumer health can be monitored independently

### Requirement 12: OpenTelemetry / Distributed Tracing

**User Story:** As a developer, I want distributed tracing support, so that I can trace messages across services using OpenTelemetry-compatible tools.

#### Acceptance Criteria

1. An ActivitySource is created for the framework (MqCSFramework)
2. Publishing a message starts an Activity (producer span) with relevant tags
3. Consuming a message starts an Activity (consumer span) linked to the producer span
4. Trace context is propagated via message headers (W3C Trace Context format)
5. Activities are compatible with OpenTelemetry exporters without additional configuration

### Requirement 13: In-Memory Transport

**User Story:** As a developer, I want an in-memory transport implementation, so that I can write tests and develop locally without requiring a running message broker.

#### Acceptance Criteria

1. The in-memory transport implements the same transport interfaces as RabbitMQ
2. Messages are routed in-process without network calls
3. Standard and RPC patterns both work with the in-memory transport
4. Registered via a simple extension method
5. Useful for unit/integration testing without requiring a running broker

---

## Non-Functional Requirements

### NFR-1: Performance

- Publishing a message should add minimal overhead over raw broker client calls (<1ms framework overhead)
- Consumer throughput should be limited only by processor logic and broker, not by framework dispatching
- No unnecessary allocations in the hot path (message receive → deserialize → dispatch)

### NFR-2: Simplicity

- The framework should have fewer types and less indirection than the reference implementation
- A developer should be able to set up a sender or consumer with fewer than 10 lines of configuration
- Clear documentation with working examples for every pattern

### NFR-3: Testability

- All public contracts are interfaces, enabling mocking in unit tests
- A test/in-memory transport implementation may be provided for integration testing without a real broker
- Processors can be tested independently of the framework

### NFR-4: Package Structure

The framework MUST be distributed as multiple focused NuGet packages:

| Package | Purpose |
|---------|---------|
| `MqCSFramework.Abstractions` | Interfaces, models, base classes (no broker dependency) |
| `MqCSFramework.RabbitMQ` | RabbitMQ transport implementation |
| `MqCSFramework.InMemory` | In-memory transport for testing/development |
| `MqCSFramework.Hosting` | BackgroundService, DI extensions, configuration binding, health checks |

### NFR-5: Compatibility

- Target: .NET 10 (LTS)
- Language: C# 14
- Nullable reference types enabled
- No platform-specific dependencies (runs on Linux, Windows, macOS)

### NFR-6: Open Source Requirements

- License: MIT (no warranty, use at your own risk)
- All dependencies must be publicly available on NuGet.org
- No proprietary references or internal package feeds
- README with quickstart guide
- CI pipeline (GitHub Actions)

---

## Constraints

- All APIs must be async-first (no sync-over-async patterns)
- Configuration must work with standard .NET configuration providers (appsettings.json, environment variables, user secrets)

---

## Resolved Decisions

| # | Question | Decision |
|---|----------|----------|
| 1 | Project/package naming | `MqCSFramework` |
| 2 | License | MIT — no warranty, permissive, no blame |
| 3 | In-memory transport in v1? | Yes — ships as `MqCSFramework.InMemory` |
| 4 | Health checks in v1? | Yes — `IHealthCheck` for broker connectivity |
| 5 | OpenTelemetry in v1? | Yes — `ActivitySource` with W3C trace propagation |

---

## Reference

- Target runtime: .NET 10 LTS (released November 2025)
- Primary broker: RabbitMQ via `RabbitMQ.Client` NuGet package
