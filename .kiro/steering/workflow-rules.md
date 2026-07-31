# Workflow Rules for MqCSFramework

## Critical Rule: Documentation-First Development

**Every change to the code MUST be reflected in the spec documents (requirements.md, design.md, tasks.md) BEFORE or IMMEDIATELY AFTER implementation.**

The objective is that the full project can be regenerated from scratch using only the spec documents. The documents are the single source of truth. If the documents don't describe something, it doesn't exist.

### What this means in practice:

1. **When making a design change** (e.g., simplifying a dispatch pattern, changing an interface, removing a class):
   - Update requirements.md if acceptance criteria changed
   - Update design.md with the new approach, code samples, and architecture
   - Update tasks.md if the implementation steps changed

2. **When adding new functionality:**
   - Add it to requirements first
   - Then design
   - Then tasks

3. **When removing functionality:**
   - Remove from all three documents

4. **Never leave the documents stale.** After every significant code change, check: "Could someone regenerate this from the docs alone?" If not, update them.

## Project-Specific Rules

- The project uses RabbitMQ only — no transport abstraction layer
- Single NuGet package: `MqCSFramework`
- Processors are registered as standard DI singletons by the developer
- Processor implementations inherit from abstract base classes (`StandardProcessor<T>`, `RpcProcessor<TReq, TRes>`) which handle deserialization internally
- Consumer dispatch uses non-generic base interfaces (`IMessageProcessor.ProcessRawAsync`, `IRpcProcessor.ProcessRawRpcAsync`) — no reflection, no separate dispatch interfaces
- The sender specifies the processor contract interface as a generic parameter for compile-time type safety
- Each sender/consumer has its own independent RabbitMQ connection
- Two sender interfaces: `IStandardSender` and `IRpcSender`
- All references to the company (xximo, XXImo) must be excluded from the project
