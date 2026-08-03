# Contributing to MqCSFramework

## How This Project Works

MqCSFramework is a **documentation-driven project**. The code is generated from specification documents — not the other way around.

The spec documents in `specs/` are the single source of truth:
- `requirements.md` — what the framework does
- `design.md` — how it's built (architecture, interfaces, implementation details)
- `tasks.md` — step-by-step implementation plan

The entire codebase can be regenerated from these documents by an AI agent (Kiro or any other). The code is a **product** of the documentation, not independent of it.

## Contributing Changes

### If you want to change behavior or add a feature:
1. Update the spec documents first (`requirements.md`, `design.md`)
2. The code should then be regenerated or updated to match

### If you want to fix a bug:
1. Identify which spec document describes the incorrect behavior
2. Fix the spec, then fix the code

### If you want to improve documentation:
1. Edit the relevant file in `specs/` (for spec/design) or `docs/` (for user-facing docs)
2. Ensure the spec documents remain regeneration-complete — someone (human or AI) should be able to rebuild the entire project from them alone

## Key Principle

> Every change to code MUST be reflected in the spec documents. If it's not in the docs, it doesn't exist.

The workflow is: **documents → code**, never code → documents.

## Coding Standards

- .NET 10, C# 14, nullable reference types enabled
- Prefer early returns over if/else chains
- Async/await consistently
- `ILogger` structured logging
- GUIDs use format "N" (no dashes)
- No credentials in committed files

## Project Structure

```
specs/                             ← Source of truth (regeneration spec)
  requirements.md                  ← What the framework does
  design.md                        ← How it's built
  tasks.md                         ← Step-by-step implementation plan
  steering/                        ← AI agent rules and project conventions
docs/                              ← User-facing documentation
src/MqCSFramework/                 ← Core shared package
src/MqCSFramework.Sender/         ← Sender package
src/MqCSFramework.Consumer/       ← Consumer package
samples/                           ← Working examples
tests/                             ← Tests
```

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
