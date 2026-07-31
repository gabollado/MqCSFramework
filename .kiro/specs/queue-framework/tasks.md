# Implementation Plan: MqCSFramework

## Overview

Implement the MqCSFramework message queue framework building on the existing skeleton (interfaces, models, exceptions, constants already defined in `MqCSFramework.Abstractions`). The implementation proceeds layer by layer: serialization → routing → senders → in-memory transport → hosting/DI → RabbitMQ transport → observability → integration tests. Each task builds incrementally on the prior step, and property-based tests (FsCheck) validate correctness properties from the design.

## Tasks

- [x] 1. Implement serialization and core utilities
  - [x] 1.1 Implement the default System.Text.Json serializer
    - Create `JsonMessageSerializer` class implementing `IMessageSerializer` in `MqCSFramework.Abstractions/Serialization/`
    - Use `System.Text.Json` with sensible defaults (camelCase, allow reading from string numbers)
    - Throw `MessageSerializationException` with context on failures (message ID, target type)
    - Implement `ContentType` property returning `"application/json"`
    - _Requirements: 4.1, 4.2, 4.3_

  - [ ]* 1.2 Write property test for serialization round-trip (Property 8)
    - **Property 8: Serialization Round-Trip**
    - **Validates: Requirements 4.1, 4.2**
    - In `MqCSFramework.Abstractions.Tests`, create `SerializationPropertyTests.cs`
    - Use FsCheck to generate arbitrary message objects, verify `Deserialize(Serialize(msg)) == msg`
    - Test with various types: primitives in records, nested objects, collections

  - [x] 1.3 Implement MessageMasker utility
    - Create `MessageMasker` internal static class in `MqCSFramework.Abstractions/` (or a shared internal location)
    - Implement `Mask(string json, HashSet<string>? maskedFields)` — case-insensitive field name matching
    - Implement `BuildFieldSet(IList<string>? fieldNames)` returning a `HashSet<string>(StringComparer.OrdinalIgnoreCase)`
    - Replace matched field values with `"***MASKED***"` using `System.Text.Json` DOM (`JsonNode`)
    - _Requirements: 9.5_

  - [ ]* 1.4 Write property test for message masking (Property 12)
    - **Property 12: Message Field Masking**
    - **Validates: Requirements 9.5**
    - In `MqCSFramework.Abstractions.Tests`, create `MessageMaskerPropertyTests.cs`
    - Verify masked fields have `"***MASKED***"` value, non-masked fields remain unchanged
    - Case-insensitive field matching property

- [x] 2. Implement MessageDispatcher
  - [x] 2.1 Implement MessageDispatcher with direct DI resolution
    - Create `MessageDispatcher` internal class in `MqCSFramework.Abstractions/Internal/`
    - Dispatch: read `mq-processor-type` header → `Type.GetType(headerValue)` → resolve from DI → call ProcessAsync
    - Cache the TMessage/TRequest type for each processor interface (resolved once from generic args)
    - NACK with `UnknownMessageTypeException` if the header is missing or the type can't be resolved from DI
    - No routing dictionary, no ProcessorRegistration, no ProcessorTypeResolver
    - Implement `DispatchStandardAsync` and `DispatchRpcAsync` methods
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.8_

  - [ ]* 2.2 Write property test for message routing correctness (Property 2)
    - **Property 2: Message Routing Correctness**
    - **Validates: Requirements 2.5, 5.3, 8.2, 8.4**
    - In `MqCSFramework.Routing.Tests`, create `MessageDispatcherPropertyTests.cs`
    - For N registered processors, verify dispatch by `mq-processor-type` header invokes exactly the correct processor
    - Verify messages without the header are rejected (NACK'd)
    - Verify unknown processor types produce NACK

  - [ ]* 2.3 Write property test for unknown message type NACK (Property 5)
    - **Property 5: Unknown Message Type NACK**
    - **Validates: Requirements 8.3**
    - In `MqCSFramework.Routing.Tests`, create `UnknownMessageTypePropertyTests.cs`
    - Verify messages with unrecognized type headers are NACK'd without requeue

- [x] 3. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Implement In-Memory transport
  - [x] 4.1 Implement InMemoryTransportConnection and InMemoryTransportChannel
    - Flesh out `InMemoryTransportConnection` in `MqCSFramework.InMemory`
    - Use `ConcurrentDictionary<string, Channel<MessageEnvelope>>` for named queues
    - `IsConnected` always returns true (in-memory is always available)
    - `CreateChannelAsync` returns an `InMemoryTransportChannel` bound to the queue dictionary
    - Implement `InMemoryTransportChannel`: `PublishAsync` writes to the named channel, `StartConsumingAsync` reads from it
    - `AcknowledgeAsync` / `NegativeAcknowledgeAsync` are no-ops for in-memory
    - _Requirements: 13.1, 13.2_

  - [x] 4.2 Implement InMemoryStandardSender
    - Create `InMemoryStandardSender : IMessageSender` in `MqCSFramework.InMemory`
    - Implement `SendAsync<TProcessor>`: resolve processor type → build `MessageEnvelope` with `mq-processor-type` header → publish
    - Implement `SendAsync<TMessage>`: build envelope with message type header → publish
    - Use `IMessageSerializer` for body serialization
    - Generate MessageId (GUID), set Timestamp, carry SendOptions metadata
    - _Requirements: 2.1, 2.2, 13.2, 13.4_

  - [x] 4.3 Implement InMemoryRpcSender
    - Create `InMemoryRpcSender : IRpcSender` in `MqCSFramework.InMemory`
    - Use `ConcurrentDictionary<string, TaskCompletionSource<byte[]>>` for pending requests
    - Set up a reply channel; correlate responses by MessageId
    - Implement timeout via `CancellationTokenSource` → throw `RpcTimeoutException`
    - _Requirements: 3.1, 3.2, 3.4, 3.6, 13.3_

  - [x] 4.4 Implement InMemoryConsumer
    - Create `InMemoryConsumer : IMessageConsumer` in `MqCSFramework.InMemory`
    - Read from the in-memory channel in a loop, dispatch to `ProcessorRouter`
    - ACK on `ProcessResult.Success`, NACK on failure
    - For RPC: serialize response and publish to reply channel
    - Support graceful stop via CancellationToken
    - _Requirements: 2.4, 2.5, 2.6, 2.7, 13.1, 13.3_

  - [ ]* 4.5 Write property test for envelope construction (Property 1)
    - **Property 1: Envelope Construction Invariant**
    - **Validates: Requirements 2.1, 2.2**
    - In `MqCSFramework.InMemory.Tests`, create `EnvelopeConstructionPropertyTests.cs`
    - Verify envelopes always contain: non-empty body, GUID MessageId, non-null MessageType, timestamp, matching optional metadata

  - [ ]* 4.6 Write property test for ACK on success (Property 3)
    - **Property 3: ACK on Successful Processing**
    - **Validates: Requirements 2.6**
    - In `MqCSFramework.InMemory.Tests`, create `AckNackPropertyTests.cs`
    - Using InMemory transport end-to-end, verify successful processor → message is ACK'd

  - [ ]* 4.7 Write property test for NACK on failure (Property 4)
    - **Property 4: NACK on Processing Failure**
    - **Validates: Requirements 2.7, 10.2**
    - In `MqCSFramework.InMemory.Tests`, add to `AckNackPropertyTests.cs`
    - Verify throwing processor → message is NACK'd with expected requeue behavior

  - [ ]* 4.8 Write property test for RPC response round-trip (Property 6)
    - **Property 6: RPC Response Round-Trip**
    - **Validates: Requirements 3.1, 3.3, 3.5, 13.3**
    - In `MqCSFramework.InMemory.Tests`, create `RpcRoundTripPropertyTests.cs`
    - Verify request→response produces correctly deserialized TResponse; processor exception → sender receives exception

  - [ ]* 4.9 Write property test for RPC concurrent correlation (Property 7)
    - **Property 7: RPC Concurrent Correlation**
    - **Validates: Requirements 3.2, 3.6**
    - In `MqCSFramework.InMemory.Tests`, create `RpcCorrelationPropertyTests.cs`
    - Fire N concurrent requests, verify each response matches its originator with zero cross-contamination

  - [ ]* 4.10 Write property test for consumer resilience (Property 9)
    - **Property 9: Consumer Resilience**
    - **Validates: Requirements 10.1**
    - In `MqCSFramework.InMemory.Tests`, create `ConsumerResiliencePropertyTests.cs`
    - Verify interleaved success/failure messages — consumer loop never terminates on processor exceptions

  - [ ]* 4.11 Write property test for correlation ID preservation (Property 14)
    - **Property 14: Correlation ID Preservation**
    - **Validates: Requirements 9.2**
    - In `MqCSFramework.InMemory.Tests`, create `CorrelationIdPropertyTests.cs`
    - Verify correlation ID from send options arrives in processor's MessageContext unchanged

- [x] 5. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Implement DI builder and hosting
  - [x] 6.1 Implement MqCSFrameworkBuilder and ServiceCollectionExtensions
    - Create `MqCSFrameworkBuilder` class and `ServiceCollectionExtensions` in `MqCSFramework.Hosting`
    - Implement `AddMqCSFramework(Action<MqCSFrameworkBuilder>)` extension method
    - Implement `AddSender`, `AddRpcSender`, `AddConsumer` — register as keyed services with per-instance connection options
    - Implement `AddInMemorySender`, `AddInMemoryConsumer` — register in-memory implementations
    - Implement `UseSerializer<T>` for custom serializer registration
    - Register `JsonMessageSerializer` as default `IMessageSerializer`
    - Register `ProcessorRouter` (which resolves processors directly from DI at runtime — no processor registration needed in the builder)
    - Processors are registered by the user as standard DI singletons: `services.AddSingleton<IMyProcessor, MyProcessorImpl>()`
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_

  - [x] 6.2 Implement ConsumerHostedService
    - Flesh out `ConsumerHostedService` in `MqCSFramework.Hosting`
    - In `ExecuteAsync`, resolve all registered `IMessageConsumer` instances and call `StartAsync`
    - On cancellation: call `StopAsync` on all consumers with a graceful timeout
    - Log consumer startup (queue name, connection name — no secrets)
    - _Requirements: 6.1, 6.2, 6.3, 6.4_

  - [x] 6.3 Implement TransportHealthCheck
    - Flesh out `TransportHealthCheck` in `MqCSFramework.Hosting`
    - Check `ITransportConnection.IsConnected` → Healthy
    - Report `Degraded` during recovery (if connection fires `ConnectionRecovered` event recently)
    - Report `Unhealthy` if disconnected
    - Implement `AddHealthChecks()` on builder to register one health check per sender/consumer connection
    - Tag each health check with the connection name
    - _Requirements: 11.1, 11.2, 11.3, 11.4_

  - [ ]* 6.4 Write unit tests for DI registration
    - In `MqCSFramework.InMemory.Tests`, create `DiRegistrationTests.cs`
    - Verify all services resolvable after `AddMqCSFramework` with in-memory configuration
    - Verify keyed service resolution for named senders/consumers
    - Verify multiple named instances coexist with independent connections
    - _Requirements: 5.1, 5.2, 5.5_

  - [ ]* 6.5 Write property test for per-connection health reporting (Property 17)
    - **Property 17: Per-Connection Health Reporting**
    - **Validates: Requirements 11.1, 11.2, 11.4**
    - In `MqCSFramework.InMemory.Tests`, create `HealthCheckPropertyTests.cs`
    - Verify each connection reports health independently — one degraded does not affect others

- [ ] 7. Implement RabbitMQ transport
  - [~] 7.1 Implement RabbitMqTransportConnection with auto-reconnect
    - Flesh out `RabbitMqTransportConnection` in `MqCSFramework.RabbitMQ`
    - Lazy init with `SemaphoreSlim`; build `ConnectionFactory` from `RabbitMqConnectionOptions`
    - Hook into `RabbitMQ.Client` connection shutdown/recovery events
    - Fire `ConnectionLost` / `ConnectionRecovered` events
    - Auto-reconnect with configurable `NetworkRecoveryInterval`
    - Implement `IAsyncDisposable` — close connection gracefully
    - _Requirements: 7.1, 7.3, 7.4_

  - [~] 7.2 Implement RabbitMqTransportChannel
    - Create `RabbitMqTransportChannel : ITransportChannel` in `MqCSFramework.RabbitMQ`
    - Wrap `RabbitMQ.Client.IChannel` operations
    - `PublishAsync` → `BasicPublishAsync` with `BasicProperties` mapped from `MessageEnvelope`
    - `StartConsumingAsync` → `BasicConsumeAsync` with async consumer callback
    - Map `BasicDeliverEventArgs` to `ReceivedMessage`
    - `AcknowledgeAsync` → `BasicAckAsync`, `NegativeAcknowledgeAsync` → `BasicNackAsync`
    - _Requirements: 2.1, 2.4, 2.6, 2.7_

  - [~] 7.3 Implement RabbitMqStandardSender
    - Create `RabbitMqStandardSender : IMessageSender, IAsyncDisposable` in `MqCSFramework.RabbitMQ`
    - Owns its own `ITransportConnection` instance (dedicated connection)
    - Lazy channel creation, reset-on-failure pattern
    - Build `MessageEnvelope` from message + `SendOptions` + processor type resolution
    - Enable publisher confirms when `ConfirmSelect = true`
    - Log message send (with body masking if configured)
    - _Requirements: 2.1, 2.2, 2.3, 7.2, 8.5, 8.6_

  - [~] 7.4 Implement RabbitMqRpcSender
    - Create `RabbitMqRpcSender : IRpcSender, IAsyncDisposable` in `MqCSFramework.RabbitMQ`
    - Owns its own `ITransportConnection` instance (dedicated connection)
    - Declare exclusive reply queue on channel init
    - Track pending requests in `ConcurrentDictionary<string, TaskCompletionSource<byte[]>>`
    - Consume reply queue; match responses by `MessageId` / `CorrelationId`
    - Handle timeout → `RpcTimeoutException`, error response → `RpcRemoteException`
    - _Requirements: 3.1, 3.2, 3.4, 3.5, 3.6_

  - [~] 7.5 Implement RabbitMqConsumer
    - Create `RabbitMqConsumer : IMessageConsumer` in `MqCSFramework.RabbitMQ`
    - Owns its own `ITransportConnection` instance (dedicated connection)
    - On `StartAsync`: connect, create channel, set prefetch, start consuming
    - Message dispatch: deserialize → route via `ProcessorRouter` → ACK/NACK
    - RPC mode: serialize response, publish to `ReplyTo` address
    - Error queue routing: check `x-death` count vs `DelayRetryLimit`, route to `ErrorQueueName` if exceeded
    - Processing timeout per message via `CancellationTokenSource`
    - _Requirements: 2.4, 2.5, 2.6, 2.7, 3.3, 10.1, 10.2, 10.3_

  - [x] 7.6 Implement RabbitMQ configuration options classes
    - Create `RabbitMqSenderOptions`, `RabbitMqRpcSenderOptions`, `RabbitMqConsumerOptions` in `MqCSFramework.RabbitMQ`
    - Each contains its own `RabbitMqConnectionOptions Connection` property (independent per endpoint)
    - Include logging, masking, and retry configuration
    - _Requirements: 5.4, 9.3, 9.4, 9.5_

  - [ ]* 7.7 Write unit tests for RabbitMQ sender channel reset (Property 11)
    - **Property 11: Sender Reset-on-Failure**
    - **Validates: Requirements 7.2**
    - In `MqCSFramework.RabbitMQ.Tests`, create `SenderResetPropertyTests.cs`
    - Mock transport to simulate failure → verify channel is reset → subsequent call succeeds
    - Verify other senders are unaffected (connection isolation)

- [~] 8. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 9. Implement OpenTelemetry tracing
  - [~] 9.1 Implement MqTracing with ActivitySource and W3C propagation
    - Create `MqTracing` internal static class (in `MqCSFramework.Abstractions` or a shared location)
    - Create `ActivitySource("MqCSFramework", "1.0.0")`
    - Implement `StartPublishActivity` — start producer span with messaging semantic convention tags
    - Implement `StartConsumeActivity` — extract parent context from headers, start consumer span
    - Implement W3C `traceparent`/`tracestate` inject/extract helpers using `MessageHeaders` constants
    - Wire into senders (publish path) and consumer (receive path)
    - _Requirements: 12.1, 12.2, 12.3, 12.4, 12.5_

  - [ ]* 9.2 Write property test for trace context propagation (Property 13)
    - **Property 13: Trace Context Propagation Round-Trip**
    - **Validates: Requirements 12.2, 12.3, 12.4**
    - In `MqCSFramework.Abstractions.Tests`, create `TraceContextPropertyTests.cs`
    - Verify inject→extract round-trip preserves trace ID and span ID in W3C format

- [ ] 10. Implement error queue routing
  - [~] 10.1 Implement dead-letter/error queue logic in RabbitMqConsumer
    - In `RabbitMqConsumer`, check `x-death` header count against `DelayRetryLimit`
    - When limit exceeded: ACK original message, publish to `ErrorQueueName`
    - When under limit: NACK without requeue (rely on DLX for retry)
    - Skip error queue logic when `DelayRetryLimit == 0` or `ErrorQueueName` is null
    - _Requirements: 10.2, 10.3_

  - [ ]* 10.2 Write property test for error queue routing (Property 10)
    - **Property 10: Error Queue Routing on Retry Exhaustion**
    - **Validates: Requirements 10.3**
    - In `MqCSFramework.InMemory.Tests`, create `ErrorQueueRoutingPropertyTests.cs`
    - Simulate retry count exceeding limit → verify message routed to error queue

- [ ] 11. Wire transport interchangeability
  - [~] 11.1 Ensure InMemory and RabbitMQ produce equivalent MessageContext
    - Review both transport implementations to confirm `ReceivedMessage` → `MessageContext` mapping is consistent
    - Standardize header propagation, timestamp handling, and correlation ID flow across transports
    - Ensure processor code is fully transport-agnostic
    - _Requirements: 1.4, 13.1_

  - [ ]* 11.2 Write property test for transport interchangeability (Property 15)
    - **Property 15: Transport Interchangeability**
    - **Validates: Requirements 1.4**
    - In `MqCSFramework.InMemory.Tests`, create `TransportInterchangeabilityPropertyTests.cs`
    - Send message through InMemory → verify MessageContext and deserialized message match expected output

- [~] 12. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 13. Integration tests with RabbitMQ (Testcontainers)
  - [~] 13.1 Set up Testcontainers RabbitMQ infrastructure
    - In `MqCSFramework.Integration.Tests`, add `Testcontainers.RabbitMq` NuGet package
    - Create a shared `RabbitMqFixture` class implementing `IAsyncLifetime`
    - Spin up a RabbitMQ container, expose connection string to tests
    - _Requirements: 1.2, 7.1_

  - [~] 13.2 Write integration test for standard messaging round-trip
    - Full end-to-end: register sender + consumer + processor via DI builder
    - Send a message → verify processor receives it with correct deserialized content and context
    - Verify ACK is sent (message no longer on queue)
    - _Requirements: 2.1, 2.4, 2.5, 2.6_

  - [~] 13.3 Write integration test for RPC round-trip
    - Register RPC sender + consumer + RPC processor
    - Send request → verify typed response returned to caller
    - Test timeout scenario (no consumer) → verify `RpcTimeoutException`
    - _Requirements: 3.1, 3.3, 3.4_

  - [~] 13.4 Write integration test for connection resilience
    - Start consumer → force-close RabbitMQ connection via management API → verify auto-reconnect
    - Verify consumer resumes processing after reconnection
    - Verify independent connections: one sender failing does not affect another sender
    - _Requirements: 7.1, 7.2, 7.3_

  - [~] 13.5 Write integration test for consumer hosted service lifecycle
    - Use `WebApplicationFactory` or `HostBuilder` to start/stop consumer hosted service
    - Verify graceful shutdown: consumers stop, pending messages not lost
    - _Requirements: 6.1, 6.2, 6.3_

  - [ ]* 13.6 Write property test for connection isolation (Property 16)
    - **Property 16: Connection Isolation**
    - **Validates: Requirements 7.1, 7.2, 7.3, 11.4**
    - In `MqCSFramework.Integration.Tests`, create `ConnectionIsolationPropertyTests.cs`
    - Register multiple senders/consumers → fail one connection → verify others remain healthy

- [~] 14. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 15. Sample projects (end-to-end demo)
  - [ ] 15.1 Create shared contracts project (MqCSFramework.Samples.Contracts)
    - Create a new class library project `samples/MqCSFramework.Samples.Contracts`
    - Define a sample processor interface: `ISampleProcessor : IRpcProcessor<SampleRequest, SampleResponse>`
    - Define `SampleRequest` record (e.g. `Name`, `Value` properties)
    - Define `SampleResponse` record (e.g. `Result`, `ProcessedAt` properties)
    - Reference only `MqCSFramework.Abstractions`
    - Add project to solution

  - [ ] 15.2 Create sample consumer console (MqCSFramework.Samples.Consumer)
    - Create a new console app project `samples/MqCSFramework.Samples.Consumer`
    - Implement `SampleProcessor : ISampleProcessor` with simple logic (echo request + timestamp)
    - Wire up using `AddMqCSFramework` builder: register consumer + processor (use InMemory or RabbitMQ based on config)
    - Use Generic Host (`Host.CreateApplicationBuilder`) with the ConsumerHostedService
    - Reference Contracts, Hosting, InMemory (and optionally RabbitMQ)
    - Add project to solution

  - [ ] 15.3 Create sample sender console (MqCSFramework.Samples.Sender)
    - Create a new console app project `samples/MqCSFramework.Samples.Sender`
    - Send an RPC message using `SendAsync<ISampleProcessor>(new SampleRequest { ... })` and print the response
    - Wire up using `AddMqCSFramework` builder: register RPC sender (use InMemory or RabbitMQ based on config)
    - Reference Contracts, Hosting, InMemory (and optionally RabbitMQ)
    - Add project to solution

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document (FsCheck with minimum 100 iterations)
- Unit tests validate specific examples and edge cases
- The existing skeleton (interfaces, models, exceptions, constants) is already in place — tasks build implementation on top
- All RabbitMQ integration tests require Testcontainers (Docker) — they can be skipped in environments without Docker

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.3"] },
    { "id": 1, "tasks": ["1.2", "1.4", "2.1"] },
    { "id": 2, "tasks": ["2.2", "2.3"] },
    { "id": 3, "tasks": ["4.1", "7.6"] },
    { "id": 4, "tasks": ["4.2", "4.3", "4.4"] },
    { "id": 5, "tasks": ["4.5", "4.6", "4.7", "4.8", "4.9", "4.10", "4.11"] },
    { "id": 6, "tasks": ["6.1"] },
    { "id": 7, "tasks": ["6.2", "6.3", "6.4", "6.5"] },
    { "id": 8, "tasks": ["7.1"] },
    { "id": 9, "tasks": ["7.2"] },
    { "id": 10, "tasks": ["7.3", "7.4", "7.5"] },
    { "id": 11, "tasks": ["7.7", "9.1"] },
    { "id": 12, "tasks": ["9.2", "10.1"] },
    { "id": 13, "tasks": ["10.2", "11.1"] },
    { "id": 14, "tasks": ["11.2"] },
    { "id": 15, "tasks": ["13.1"] },
    { "id": 16, "tasks": ["13.2", "13.3", "13.4", "13.5", "13.6"] },
    { "id": 17, "tasks": ["15.1"] },
    { "id": 18, "tasks": ["15.2", "15.3"] }
  ]
}
```
