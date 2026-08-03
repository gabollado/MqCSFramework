# Implementation Plan: MqCSFramework

## Overview

Build a single-package RabbitMQ messaging framework for .NET 10 from scratch. The framework provides compile-time type-safe sending, automatic consumer dispatch via DI resolution from message headers, and independent connection management per sender/consumer. Two messaging patterns are supported: Standard (fire-and-forget) and RPC (request-reply).

The implementation language is C# 14 / .NET 10.

## Tasks

- [ ] 1. Set up solution structure and core project scaffolding
  - [ ] 1.1 Create the src/MqCSFramework class library project and tests/MqCSFramework.Tests xUnit test project
    - Delete old directories (`src/MqCSFramework.Hosting`, `src/MqCSFramework.InMemory`, `src/MqCSFramework.RabbitMQ`, `tests/MqCSFramework.Abstractions.Tests`, `tests/MqCSFramework.InMemory.Tests`, `tests/MqCSFramework.Integration.Tests`, `tests/MqCSFramework.RabbitMQ.Tests`, `tests/MqCSFramework.Routing.Tests`)
    - Create `src/MqCSFramework/MqCSFramework.csproj` targeting net10.0 with package references: RabbitMQ.Client 7.x, Microsoft.Extensions.Hosting, Microsoft.Extensions.DependencyInjection, Microsoft.Extensions.Logging
    - Create `tests/MqCSFramework.Tests/MqCSFramework.Tests.csproj` targeting net10.0 with references: xUnit, FsCheck.Xunit, Testcontainers (for RabbitMQ), the main project reference
    - Update `MqCSFramework.slnx` to include both projects under the appropriate solution folders
    - Verify the solution builds with `dotnet build`
    - _Requirements: Non-Functional Requirements (net10.0, C# 14, nullable enabled)_

- [ ] 2. Implement public interfaces, models, and exceptions
  - [ ] 2.1 Create processor contract interfaces and MessageContext
    - Create `IMessageProcessor<TMessage>` interface with `ProcessAsync(TMessage message, MessageContext context, CancellationToken ct)` method
    - Create `IRpcProcessor<TRequest, TResponse>` interface with `ProcessAsync(TRequest request, MessageContext context, CancellationToken ct)` returning `Task<TResponse>`
    - Create `MessageContext` sealed record with MessageId, CorrelationId, Timestamp, Pattern, Headers properties
    - _Requirements: 1.2, 2.2, 3.5_

  - [ ] 2.2 Create sender interfaces
    - Create `IStandardSender` interface with `SendAsync<TProcessor, TMessage>(TMessage message, SendOptions? options, CancellationToken ct)` returning `Task<string>`
    - Create `IRpcSender` interface with `SendAsync<TProcessor, TResponse, TRequest>(TRequest request, RpcOptions? options, CancellationToken ct)` returning `Task<TResponse>`
    - Enforce generic constraints: `TProcessor : IMessageProcessor<TMessage>` and `TProcessor : IRpcProcessor<TRequest, TResponse>`
    - _Requirements: 1.1, 1.2, 2.1, 2.2_

  - [ ] 2.3 Create configuration options classes
    - Create `RabbitMqConnectionOptions` (HostName, Port, UserName, Password, VirtualHost, UseSsl, ClientProvidedName)
    - Create `StandardSenderOptions` (Connection, Exchange, RoutingKey)
    - Create `RpcSenderOptions` (Connection, Exchange, RoutingKey, Timeout)
    - Create `ConsumerOptions` (Connection, QueueName, PrefetchCount, MaxRetries, DeadLetterExchange, DeadLetterRoutingKey, SuppressMessageBodyLogging, MaskedFields)
    - Create `SendOptions` (RoutingKey, CorrelationId, AdditionalHeaders)
    - Create `RpcOptions` (RoutingKey, CorrelationId, Timeout, AdditionalHeaders)
    - _Requirements: 4.1, 5.2, 5.3, 5.4, 8.3, 8.4, 9.3_

  - [ ] 2.4 Create exception types
    - Create `RpcTimeoutException` with CorrelationId and Timeout properties
    - Create `RpcRemoteException` with CorrelationId and RemoteExceptionType properties
    - Create `MessageSerializationException` with MessageId property
    - _Requirements: 2.7, 2.8, 7.2_

- [ ] 3. Implement connection management
  - [ ] 3.1 Create RabbitMqConnection internal class
    - Implement lazy connection and channel creation via `GetChannelAsync(CancellationToken ct)`
    - Use `ConnectionFactory` with `AutomaticRecoveryEnabled = true`
    - Map all `RabbitMqConnectionOptions` properties to the factory (host, port, credentials, virtual host, SSL, client name)
    - Implement `IAsyncDisposable` for graceful channel and connection close
    - _Requirements: 4.1, 4.3, 4.4_

- [ ] 4. Implement standard sender
  - [ ] 4.1 Create RabbitMqStandardSender internal class implementing IStandardSender
    - Accept `RabbitMqConnection` and `StandardSenderOptions` via constructor
    - Serialize message to UTF-8 JSON using `System.Text.Json`
    - Build `BasicProperties` with MessageId (GUID), CorrelationId, Timestamp, ContentType, and headers (`mq-processor-type` = AssemblyQualifiedName of TProcessor, `mq-pattern` = "standard")
    - Merge `SendOptions.AdditionalHeaders` if provided
    - Publish via `channel.BasicPublishAsync` to configured exchange/routing key
    - Return the generated MessageId
    - Log the send operation with correlation ID
    - _Requirements: 1.3, 1.4, 1.5, 1.6, 7.1, 8.1, 8.2_

  - [ ]* 4.2 Write property test for message envelope correctness (standard sender)
    - **Property 1: Message Envelope Correctness**
    - Generate random message objects and processor types, verify the published message has correct `mq-processor-type` header, `mq-pattern` = "standard", valid GUID MessageId, non-zero Timestamp, and non-empty CorrelationId
    - **Validates: Requirements 1.3, 1.4, 1.5**

  - [ ]* 4.3 Write property test for serialization round-trip
    - **Property 2: Serialization Round-Trip**
    - Generate random record-type messages, serialize to JSON bytes and deserialize back, verify equality
    - **Validates: Requirements 1.6, 7.1**

- [ ] 5. Implement RPC sender
  - [ ] 5.1 Create RabbitMqRpcSender internal class implementing IRpcSender
    - Accept `RabbitMqConnection` and `RpcSenderOptions` via constructor
    - Maintain a `ConcurrentDictionary<string, TaskCompletionSource<byte[]>>` for pending RPC calls
    - Set up a Direct Reply-to consumer on the connection's channel to receive responses
    - On send: serialize request, set headers (`mq-processor-type`, `mq-pattern` = "rpc"), set `ReplyTo` = "amq.rabbitmq.reply-to", publish
    - Await response with configurable timeout; throw `RpcTimeoutException` on expiry
    - Deserialize response envelope; throw `RpcRemoteException` if `IsError` is true
    - Implement `HandleReply` method called by the reply consumer
    - _Requirements: 2.3, 2.4, 2.5, 2.6, 2.7, 2.8_

  - [ ]* 5.2 Write property test for RPC round-trip correlation
    - **Property 4: RPC Round-Trip Correlation**
    - Generate random CorrelationIds and response objects, simulate response delivery via HandleReply, verify the sender receives the correctly deserialized TResponse matching the CorrelationId
    - **Validates: Requirements 2.5, 2.6**

  - [ ]* 5.3 Write property test for RPC error propagation
    - **Property 5: RPC Error Propagation**
    - Generate random exception messages, simulate error response via HandleReply with IsError=true, verify RpcRemoteException is thrown containing the original error message
    - **Validates: Requirements 2.8**

- [ ] 6. Checkpoint - Verify sender implementations
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 7. Implement consumer message dispatch
  - [ ] 7.1 Create MqConsumer internal class
    - Accept `ConsumerOptions`, `IServiceProvider`, `ILogger<MqConsumer>` via constructor
    - Implement `StartAsync`: create connection, create channel, set BasicQos (prefetchCount), register `AsyncEventingBasicConsumer`, subscribe to `ReceivedAsync`, call `BasicConsumeAsync` with `autoAck: false`
    - Implement `DispatchMessage`: read `mq-processor-type` header → `Type.GetType` → resolve from DI → deserialize body → call `ProcessAsync`
    - For standard pattern: ACK on success, handle failures per retry logic
    - For RPC pattern: call ProcessAsync, serialize `RpcResponseEnvelope` (success or error), publish to ReplyTo queue, ACK
    - Handle missing headers (NACK without requeue), unresolvable types (NACK without requeue), deserialization failures (NACK without requeue)
    - Track retry count via `mq-retry-count` header; dead-letter when count >= MaxRetries
    - Implement `IAsyncDisposable` for graceful channel/connection close
    - _Requirements: 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 9.1, 9.2, 9.3_

  - [ ] 7.2 Create RpcResponseEnvelope internal record
    - Properties: `IsError`, `Payload` (byte[]), `ErrorMessage`, `ErrorType`
    - Used by consumer to wrap processor results/errors before publishing to reply queue
    - _Requirements: 2.5, 2.8_

  - [ ]* 7.3 Write property test for consumer dispatch correctness
    - **Property 3: Consumer Dispatch Correctness**
    - Generate random registered processor types and matching messages, mock DI resolution, verify the correct processor's ProcessAsync is called with the deserialized message and a valid MessageContext
    - **Validates: Requirements 3.2, 3.3, 3.4, 3.5**

  - [ ]* 7.4 Write property test for processor fault tolerance
    - **Property 6: Processor Fault Tolerance**
    - Generate random exceptions thrown by processors, verify the consumer NACKs the message and does not crash (continues processing)
    - **Validates: Requirements 9.1, 9.2**

  - [ ]* 7.5 Write property test for dead-letter routing
    - **Property 7: Dead-Letter Routing on Retry Exhaustion**
    - Generate messages with varying retry counts (some below, some at/above MaxRetries), verify messages at/above threshold are published to dead-letter exchange instead of being requeued
    - **Validates: Requirements 9.3**

- [ ] 8. Implement logging and sensitive field masking
  - [ ] 8.1 Create LogMaskingHelper internal static class
    - Accept a message body string and a list of masked field names
    - Parse JSON, replace values of matching fields with `"***MASKED***"`
    - Return the masked JSON string for logging purposes
    - Used by the consumer when `SuppressMessageBodyLogging` is false
    - _Requirements: 8.3, 8.4_

  - [ ]* 8.2 Write property test for sensitive field masking
    - **Property 8: Sensitive Field Masking**
    - Generate random JSON objects with some fields in the masked list and some not, verify masked fields have `"***MASKED***"` values and non-masked fields are preserved
    - **Validates: Requirements 8.4**

- [ ] 9. Implement the DI builder and hosted service
  - [ ] 9.1 Create MqBuilder class and ServiceCollectionExtensions
    - Implement `AddMqCSFramework(Action<MqBuilder> configure)` extension method on `IServiceCollection`
    - Implement `AddSender(string name, Action<StandardSenderOptions> configure)` — registers a keyed `IStandardSender` singleton with its own `RabbitMqConnection`
    - Implement `AddRpcSender(string name, Action<RpcSenderOptions> configure)` — registers a keyed `IRpcSender` singleton with its own `RabbitMqConnection`
    - Implement `AddConsumer(string name, Action<ConsumerOptions> configure)` — stores consumer registrations
    - Implement `Build()` — registers `ConsumerHostedService` as a hosted service if any consumers are configured
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_

  - [ ] 9.2 Create ConsumerHostedService internal class extending BackgroundService
    - Accept consumer registrations, `IServiceProvider`, and `ILoggerFactory` via constructor
    - In `ExecuteAsync`: create `MqConsumer` instances for each registration, start them all in parallel
    - On cancellation token trigger: dispose all consumers gracefully
    - _Requirements: 6.1, 6.2, 6.3_

  - [ ]* 9.3 Write unit tests for DI registration and builder
    - Test that `AddSender` registers keyed `IStandardSender` resolvable via `GetRequiredKeyedService`
    - Test that `AddRpcSender` registers keyed `IRpcSender` resolvable via `GetRequiredKeyedService`
    - Test that `AddConsumer` results in `ConsumerHostedService` being registered as a hosted service
    - Test that multiple senders/consumers can be registered
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 6.3_

- [ ] 10. Checkpoint - Verify core framework builds and unit tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 11. Create sample projects
  - [ ] 11.1 Create samples/MqCSFramework.Samples.Contracts class library
    - Define sample message records: `OrderMessage`, `StockRequest`, `StockResponse`
    - Define processor contract interfaces: `IOrderProcessor : IMessageProcessor<OrderMessage>`, `IStockProcessor : IRpcProcessor<StockRequest, StockResponse>`
    - Reference the main `MqCSFramework` project
    - _Requirements: 1.2, 2.2_

  - [ ] 11.2 Create samples/MqCSFramework.Samples.Sender console app
    - Configure `AddMqCSFramework` with a standard sender ("orders") and an RPC sender ("stock")
    - Demonstrate sending a standard message and an RPC request with response handling
    - Reference both the main project and the contracts sample project
    - _Requirements: 1.1, 2.1, 5.1, 5.2, 5.3, 5.5_

  - [ ] 11.3 Create samples/MqCSFramework.Samples.Consumer console app
    - Register processor implementations as DI singletons
    - Configure `AddMqCSFramework` with consumers for orders and stock queues
    - Implement `OrderProcessor` (standard) and `StockProcessor` (RPC)
    - Run as hosted service
    - Reference both the main project and the contracts sample project
    - _Requirements: 3.1, 3.6, 5.1, 5.4, 6.1_

- [ ] 12. Integration tests with Testcontainers
  - [ ] 12.1 Write integration tests for standard message flow
    - Use Testcontainers to spin up a RabbitMQ container
    - Create exchange and queue with binding
    - Send a standard message via `IStandardSender`, consume it via a real `MqConsumer`, verify the processor receives the correct deserialized message
    - _Requirements: 1.3, 1.4, 1.5, 1.6, 3.2, 3.3, 3.4, 3.5_

  - [ ] 12.2 Write integration tests for RPC flow
    - Use the same Testcontainers RabbitMQ instance
    - Send an RPC request via `IRpcSender`, have a consumer process it and return a response, verify the sender receives the correct typed response
    - Test timeout behavior (no consumer, short timeout → `RpcTimeoutException`)
    - Test error propagation (processor throws → `RpcRemoteException` at sender)
    - _Requirements: 2.3, 2.4, 2.5, 2.6, 2.7, 2.8_

  - [ ] 12.3 Write integration test for dead-letter routing
    - Configure a consumer with MaxRetries = 2 and a dead-letter exchange
    - Send a message that always fails processing
    - Verify the message ends up on the dead-letter queue after retry exhaustion
    - _Requirements: 9.1, 9.2, 9.3_

- [ ] 13. Final checkpoint - Full solution verification
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document
- Unit tests validate specific examples and edge cases
- The design uses C# directly — no language selection needed
- Old multi-package directories must be cleaned up in task 1.1 before creating the new single-package structure
- All sample projects should be added to the solution file under the `/samples/` folder

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["2.1", "2.2", "2.3", "2.4"] },
    { "id": 2, "tasks": ["3.1"] },
    { "id": 3, "tasks": ["4.1", "5.1"] },
    { "id": 4, "tasks": ["4.2", "4.3", "5.2", "5.3"] },
    { "id": 5, "tasks": ["7.1", "7.2", "8.1"] },
    { "id": 6, "tasks": ["7.3", "7.4", "7.5", "8.2"] },
    { "id": 7, "tasks": ["9.1"] },
    { "id": 8, "tasks": ["9.2", "9.3"] },
    { "id": 9, "tasks": ["11.1"] },
    { "id": 10, "tasks": ["11.2", "11.3"] },
    { "id": 11, "tasks": ["12.1", "12.2", "12.3"] }
  ]
}
```
