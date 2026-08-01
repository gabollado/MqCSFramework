# MqCSFramework Documentation

Welcome to the MqCSFramework documentation. This guide covers everything you need to get started, configure, and use the framework in your projects.

## Documentation Index

| Document | Description |
|----------|-------------|
| [Overview](overview.md) | What MqCSFramework is, its use cases, and architecture |
| [Quick Start](quickstart.md) | Get up and running in minutes |
| [Configuration Reference](configuration.md) | Detailed configuration options and appsettings.json reference |
| [API Reference](api-reference.md) | Public interfaces, classes, and methods |
| [Samples](samples.md) | Running the included samples and building your own |

## What is MqCSFramework?

MqCSFramework is a lightweight RabbitMQ messaging framework for .NET 10 that makes it simple to send and consume messages with compile-time type safety. It supports two messaging patterns:

- **Standard** (fire-and-forget) — publish a message, no response expected
- **RPC** (request-reply) — publish a request, await a typed response

The framework handles connection management, serialization, error handling, retries, and dead-letter routing so you can focus on your business logic.
