# Workflow Rules for MqCSFramework

## Critical Rule: Documentation-First Development

**Every change to the code MUST be reflected in the spec documents (requirements.md, design.md, tasks.md) BEFORE implementation. Update docs FIRST, then code.**

The objective is that the full project can be regenerated from scratch using only the spec documents. The documents are the single source of truth. If the documents don't describe something, it doesn't exist.

### Enforcement:

1. **BEFORE making any code change** — update the relevant spec document first (design.md for structural changes, requirements.md for behavior changes)
2. **Never write code without updating docs in the same action**
3. **If you forget — stop, update docs immediately before continuing**
4. **No commits should ever contain code changes without corresponding doc changes**

This rule exists because the developer WILL delete all code and regenerate from docs alone. If a change isn't in the docs, it will be lost.

## Project-Specific Rules

- The project uses RabbitMQ only — no transport abstraction layer
- Three NuGet packages: `MqCSFramework` (core/shared), `MqCSFramework.Sender`, `MqCSFramework.Consumer`
- Processors are registered as standard DI singletons by the developer
- Processor implementations inherit from abstract base classes (`StandardProcessor<T>`, `RpcProcessor<TReq, TRes>`) which handle deserialization internally
- Consumer dispatch uses non-generic base interfaces (`IMessageProcessor.ProcessRawAsync`, `IRpcProcessor.ProcessRawRpcAsync`) — no reflection, no separate dispatch interfaces
- The sender specifies the processor contract interface as a generic parameter for compile-time type safety
- Each sender/consumer has its own independent RabbitMQ connection
- Two sender interfaces: `IStandardSender` and `IRpcSender`
- All references to the company (xximo, XXImo) must be excluded from the project
- Prefer early returns over if/else chains — if a branch ends with return, throw, or exits, don't use else
