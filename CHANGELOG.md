# Changelog

All notable changes to MqCSFramework will be documented in this file.

## [0.2.0-alpha] - 2026-08-03

### Added
- Split into three packages: `MqCSFramework` (core), `MqCSFramework.Sender`, `MqCSFramework.Consumer`
- CorrelationId scoped logging via `logger.CorrelationScope(correlationId)` extension method
- Per-message cancellation tokens: standard uses `ProcessingTimeoutMs`, RPC uses `mq-cancellation-deadline` header
- `MessageHelpers` static class extracted from consumer for cleaner code
- `RpcRequestResponseHandler` extracted from RPC sender
- `LogMaskingHelper` for sensitive field masking in log output
- `AddMqSendersFromConfiguration()` and `AddMqConsumersFromConfiguration()` for one-line config binding
- Serilog per-namespace log level override (`"MqCSFramework": "Debug"`) to control body logging

### Changed
- CorrelationId is now a mandatory parameter on sender interfaces (not in options)
- Removed `SuppressMessageBodyLogging` — body logging controlled via Serilog log levels
- GUIDs use format "N" (no dashes) throughout
- Early-return pattern applied (no unnecessary else blocks)

## [0.1.0-alpha] - 2026-08-01

### Added
- Initial working framework: standard (fire-and-forget) and RPC (request-reply) messaging
- Compile-time type safety via processor contract interfaces
- Consumer auto-resolves processors from DI using `mq-processor-type` header
- Independent RabbitMQ connections per sender/consumer
- Abstract base classes (`StandardProcessor<T>`, `RpcProcessor<TReq, TRes>`) with zero-reflection dispatch
- Builder pattern DI registration (`AddMqCSFramework`)
- Retry logic with configurable max retries and dead-letter exchange routing
- Serilog structured logging with file + console sinks
- Configuration from `appsettings.json` with local override support
- Sample projects (contracts, sender, consumer) with CloudAMQP integration
- MIT license
