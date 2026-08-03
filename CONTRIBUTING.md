# Contributing to MqCSFramework

Thanks for your interest in contributing! Here's how to get started.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR-USERNAME/MqCSFramework.git`
3. Create a feature branch: `git checkout -b feature/my-feature`
4. Make your changes
5. Ensure the solution builds: `dotnet build MqCSFramework.slnx`
6. Commit with a clear message
7. Push and open a Pull Request

## Development Setup

- .NET 10 SDK
- A RabbitMQ instance for integration testing (local Docker or CloudAMQP free tier)
- Copy `appsettings.local.json` in the sample projects with your connection credentials (git-ignored)

## Coding Standards

- C# 14, nullable reference types enabled
- Prefer early returns over if/else chains
- Async/await consistently — no sync-over-async
- Use `ILogger` structured logging (not string interpolation in log calls)
- All public types need XML documentation comments
- Use meaningful names that reveal intent
- Keep methods under ~30 lines where practical
- GUIDs use format "N" (no dashes): `Guid.NewGuid().ToString("N")`

## Pull Request Guidelines

- One logical change per PR
- Include a clear description of what changed and why
- Ensure the solution builds with zero errors and warnings
- Update documentation (spec docs or user docs) if behavior changes
- Don't include credentials or environment-specific config

## Project Structure

```
src/MqCSFramework/           ← Core shared package (interfaces, models, exceptions)
src/MqCSFramework.Sender/   ← Sender implementations + DI registration
src/MqCSFramework.Consumer/ ← Consumer implementations + DI registration
tests/MqCSFramework.Tests/  ← Unit and integration tests
samples/                     ← Working example projects
docs/                        ← User documentation
```

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
