# Technical Audit: MqCSFramework (Revision 2 — Source-Verified)

**Repository:** https://github.com/gabollado/MqCSFramework
**Auditor scope:** Current state only (no commit-history/evolution analysis, per original request)
**Date of audit:** August 13, 2026

## Revision Notice

The original version of this audit was built from the README, CHANGELOG, and public docs (GitHub blocks automated directory-tree browsing for this tool). All source-level claims in that version were explicitly labeled *inferred*. The person subsequently uploaded a full local clone (working tree + `.git` + build output), which was extracted and read directly — every `.cs` file in `src/` and `tests/`, both `.csproj` trees, the CI workflow, `NuGet.Config`, and the `specs/` directory.

This revision **replaces** the prior report. Three things changed as a result of reading the real source:

1. **One claim was wrong and is retracted below**: the original report treated "no reconnection strategy" as the single biggest gap. The actual code sets `AutomaticRecoveryEnabled = true` and `TopologyRecoveryEnabled = true` on every connection and layers a self-healing channel check on top. This is a real, standard mechanism — the concern was unfounded.
2. **Several inferred concerns were confirmed as fact**, several with more precision or severity than the inferred version suggested (message persistence, test coverage, dead-letter/retry duplicate-delivery window).
3. **New findings emerged that were invisible from documentation alone**, including a confirmed documentation/code drift (two independent instances), a package-versioning defect, a consumer-isolation gap, and the discovery that this codebase is explicitly built via an AI-agent spec-driven workflow (Kiro), which is stated directly in `CONTRIBUTING.md` and `specs/steering/workflow-rules.md`.

Sections below are marked **[confirmed]**, **[corrected]**, or **[new]** where the source review changed the prior assessment. Sections not marked were not materially affected and are carried forward with light editing.

---

## 1. Executive Summary

MqCSFramework is a **~2-week-old, single-maintainer, alpha-stage (git tag `v0.3.0-alpha`)** .NET 10 library wrapping RabbitMQ.Client to provide compile-time-typed "fire-and-forget" and "RPC" messaging. Having now read the actual source, the core assessment holds: this is a **thin, opinionated convenience layer over RabbitMQ.Client**, not a novel solution — everything it does is already solved, at far greater maturity, by MassTransit, Rebus, EasyNetQ, or NServiceBus.

What's new after reading the code: **the implementation quality is genuinely good** — clean async patterns, correct use of `SemaphoreSlim`-guarded lazy connection init, proper `IAsyncDisposable` cleanup, sensible layered exception handling, and idiomatic use of C# 14 features. This is better-written code than the "alpha, 17 commits, single maintainer" profile would predict. At the same time, direct source review surfaced **concrete, confirmed defects that documentation review alone could not have found**: messages are never marked persistent despite durable queues (real data-loss risk on broker restart), the entire automated test suite is three trivial DI-resolution checks against ten explicitly-specified correctness properties in the design doc, one misconfigured consumer can take the whole host down, and the user-facing docs have already drifted from the shipped API in two confirmed places — notable because this project's own contribution workflow explicitly states "if it's not in the docs, it doesn't exist."

**Verdict is unchanged: use only for learning.** The reasons shifted — from "unproven and under-specified" to "well-built but with specific, fixable correctness gaps and a documentation-sync process that isn't yet holding up in practice."

---

## 2. Project Overview

Unchanged from the original assessment: the problem (typed messaging over RabbitMQ in .NET) is real and clearly scoped; the "event-driven architecture" use case remains honestly caveated in the docs as needing configuration beyond what's demonstrated.

### Provenance **[new]**

`CONTRIBUTING.md` and `specs/steering/workflow-rules.md` state directly that this is a **documentation-driven, AI-agent-generated codebase**: *"The entire codebase can be regenerated from these documents by an AI agent (Kiro or any other). The code is a product of the documentation, not independent of it."* The `specs/` directory (`requirements.md`, `design.md`, `tasks.md`, `steering/workflow-rules.md`) follows the exact file-naming convention of Kiro-style spec-driven development. The stated rule is: *"Every change to the code MUST be reflected in the spec documents... BEFORE implementation... If the documents don't describe something, it doesn't exist."*

This is stated as fact, not speculation — it's the project's own description of itself. It's relevant to a due-diligence audit for two reasons: it plausibly explains why the code is unusually consistent for a 2-week-old solo project (much of it is templated from a design spec rather than accreted organically), and it sets up a direct, falsifiable test of the project's own claimed process discipline — a test the project currently fails in two places (§6, §8).

Separately, `specs/steering/workflow-rules.md` contains the line *"All references to the company (xximo, XXImo) must be excluded from the project."* Noted factually and without further speculation: this suggests the framework may have originated from or alongside internal/company work rather than being designed from inception as a general-purpose open-source library. This doesn't change the technical assessment, but it's a relevant provenance fact for anyone evaluating long-term maintenance intent.

---

## 3. Architecture Analysis

Unchanged. The point-to-point/RPC architecture diagram from the original report holds and is now additionally confirmed line-for-line against `IStandardSender`, `IRpcSender`, `RabbitMqStandardSender`, `RabbitMqRpcSender`, and `MqConsumer`. Each sender/consumer does own an independent `RabbitMqConnection`; scaling is delegated entirely to RabbitMQ's competing-consumers behavior, as documented.

```
┌────────────────────────────────────────────────────────────────┐
│                     Shared Contracts Project                    │
│  IOrderProcessor : IMessageProcessor<OrderMessage>               │
│  IStockProcessor : IRpcProcessor<StockRequest, StockResponse>   │
└────────────────────────────────────────────────────────────────┘
                 ▲                                    ▲
┌────────────────┴───────────────┐   ┌─────────────────┴──────────────┐
│         Sender Service          │   │        Consumer Service         │
│  IStandardSender (keyed DI)      │   │  ConsumerHostedService          │
│   .SendAsync<IOrderProcessor,    │   │   → MqConsumer per registration │
│      OrderMessage>(msg, corrId)──┼──►│   → Type.GetType(header) + DI   │
│                                  │   │     resolve → ProcessAsync      │
│  IRpcSender (keyed DI)            │   │   → ACK / republish-retry /     │
│   .SendAsync<IStockProcessor,    │◄──┼──   DLX on exhaustion          │
│      StockResponse,              │   │                                  │
│      StockRequest>(req, corrId)  │   │  exclusive reply queue per       │
└──────────────────────────────────┘   │  sender instance, correlated by │
        │ AutomaticRecoveryEnabled=true │  correlationId in a             │
        ▼                               │  ConcurrentDictionary<string,TCS>│
┌────────────────────────────────────────────────────────────────┐
│                        RabbitMQ Broker                          │
│   default exchange only; durable queues; messages published     │
│   WITHOUT DeliveryMode=Persistent (see §4)                       │
└────────────────────────────────────────────────────────────────┘
```

---

## 4. RabbitMQ Implementation Audit

| Aspect | Status | Evidence |
|---|---|---|
| Exchange configuration | Default exchange only | Confirmed in `RabbitMqStandardSender`/`RabbitMqRpcSender` — `_options.Exchange` defaults to `""` and no sample or test exercises a topic/fanout exchange. |
| Reconnection strategy | **[corrected] Present and standard** | `RabbitMqConnection.CreateConnectionAsync` sets `AutomaticRecoveryEnabled = true, TopologyRecoveryEnabled = true` on the `ConnectionFactory`. `GetChannelAsync` additionally checks `_channel is { IsOpen: true }` / `_connection is null or { IsOpen: false }` and transparently recreates on top of that, under a `SemaphoreSlim(1,1)` lock. This is a legitimate, idiomatic reconnection design — the prior report's "no reconnection story" claim was **wrong** and is retracted. Two residual gaps remain: no explicit `NetworkRecoveryInterval` override (uses the client default), and no event hooks (`ConnectionShutdown`, `RecoverySucceeded`) surfaced to the host application — so recovery happens, but silently, with no way for an operator to observe it happening. |
| Message persistence | **[corrected — confirmed absent, high severity]** | `RabbitMqStandardSender.SendAsync` and `RabbitMqRpcSender.SendAsync` both construct `BasicProperties` without setting `DeliveryMode`/`Persistent`. RabbitMQ.Client defaults this to **transient (delivery mode 1)**. Queues are declared `durable: true` (`MqConsumer.StartAsync`), but **queue durability only protects the queue's existence across a broker restart — it does not persist the messages inside it.** As written, every message sent through this framework is lost if the broker restarts or crashes while the message is still queued, regardless of `MaxRetries`, `DeadLetterExchange`, or any consumer-side configuration. This directly undermines the README's resilience claim and is the single most consequential defect found in this audit — and it is a two-line fix (`Persistent = true` on `BasicProperties`). |
| Retry mechanism | Present, header-driven | `MqConsumer.HandleFailureAsync` reads `mq-retry-count`, compares against `ConsumerOptions.MaxRetries` (default 3), and republishes with an incremented header on failure. |
| Dead-letter queues | Present, opt-in; **confirmed silent-drop risk [confirmed]** | If `DeadLetterExchange` is unset, exhausted retries are NACK'd without requeue (silent loss by design, as before). New: even when a DLX *is* configured, the DLX publish uses `BasicPublishAsync(..., mandatory: false, ...)` with **no publisher confirms**. If the configured DLX name is wrong or not yet created, RabbitMQ will silently drop the message and the framework has no way to know — it logs a warning and ACKs the original regardless. |
| Retry/DLQ atomicity **[new]** | Narrow duplicate-delivery window | Both the retry path and the DLX path follow the same pattern: `BasicPublishAsync` (the retry copy or DLX copy) immediately followed by `BasicAckAsync` on the original delivery tag. These are two separate network operations, not a transaction. If the process crashes between them, the original message is redelivered on reconnect (since it was never acked) *and* the republished copy already exists — a real, if narrow, duplicate-processing window. This is a specific mechanism behind the general "no idempotency guarantee" concern raised in the original report. |
| RPC correlationId collision **[new]** | Moderate, undocumented | `correlationId` became a mandatory, caller-supplied parameter on both sender interfaces (per `CHANGELOG` 0.2.0-alpha). `RpcRequestResponseHandler` keys its pending-request table as `ConcurrentDictionary<string, TaskCompletionSource<byte[]>>` by that same string. If a caller issues two concurrent RPC calls on the same `IRpcSender` instance with the same `correlationId` (accidental reuse, not framework-generated), the second call's registration silently overwrites the first's dictionary entry. Worse: the *first* call's own timeout-cleanup closure will then `TryRemove` and fault whatever TCS currently occupies that key — meaning **one call's timeout can incorrectly fail the other call's result**. Nothing in `docs/configuration.md` or `docs/api-reference.md` warns that `correlationId` must be unique per in-flight call on a given sender instance. |
| Consumer resolution via `Type.GetType()` **[new]** | Moderate, worth a documented boundary | The consumer resolves processors via `Type.GetType(processorTypeName)` where `processorTypeName` is the `mq-processor-type` header — attacker-controlled if anything untrusted can publish to the queue. Because the resolved type is only ever used as a lookup key into `IServiceProvider.GetService(processorType)`, the practical exposure is bounded to "which already-registered processor handles this payload," not arbitrary code execution — but it does mean any queue publisher can redirect a message body to any processor registered in that consumer's DI container, and there's no reference-check verifying the payload was actually produced for that processor. Reasonable for a trusted-network broker; worth an explicit trust-boundary note in the docs. |
| Serialization | System.Text.Json, confirmed | `StandardProcessor<T>.ProcessRawAsync` / `RpcProcessor<TReq,TRes>.ProcessRawRpcAsync` both deserialize via `JsonSerializer.Deserialize`, throwing `MessageSerializationException` on null result — clean, consistent with docs. |
| Correlation IDs | Confirmed first-class | Mandatory sender parameter, propagated into `MessageContext` and into a `BeginScope`-based `ILogger` scope (`LoggingExtensions.CorrelationScope`) for automatic log correlation — genuinely good design, correctly implemented. |
| Event versioning | Still absent | Confirmed by absence across all reviewed files — no upcasting, no schema-version header, no migration story. |

**Determination (revised):** RabbitMQ usage is correct for its stated point-to-point/RPC scope and *not* overengineered. The reconnection story is sound. The **persistence gap is the standout defect** — a concrete, high-severity, easily-fixed bug rather than an abstract "underspecified resilience" concern.

---

## 5. API and Inter-Application Communication Audit

Unchanged in substance. Confirmed from source: `RpcRequestResponseHandler.PublishAndAwaitReplyAsync` implements timeout via `CancellationTokenSource.CreateLinkedTokenSource` + `cts.Token.Register(...)` cleanup — this is correct, idiomatic async cancellation handling with proper cleanup of the pending-request dictionary on both success and timeout paths (the `catch (Exception) when (!tcs.Task.IsCompleted)` guard is a genuinely careful touch). No circuit breaker is present anywhere in the reviewed source (confirmed, not just inferred). No Polly or comparable resilience-pipeline dependency appears in any `.csproj`.

---

## 6. Code Quality Audit **[confirmed — full source reviewed]**

This section previously carried the audit's largest caveat (inferred only). With the full `src/` tree read, here is a direct assessment.

### What's genuinely good

- **Correct, careful async/concurrency code.** `RabbitMqConnection.GetChannelAsync` uses double-checked locking under a `SemaphoreSlim(1,1)` correctly (check → lock → re-check → act), which is easy to get subtly wrong and is done right here. `RpcRequestResponseHandler` correctly links cancellation tokens, registers cleanup callbacks, and guards against double-completing a `TaskCompletionSource`.
- **Consistent, idiomatic C# 14 / .NET 10 usage**: primary constructors, collection expressions (`= []`), required members on `MessageContext`, `sealed` on every internal implementation class, nullable reference types enabled solution-wide (`Directory.Build.props`).
- **Clean separation via non-generic base interfaces** (`IMessageProcessor`/`IRpcProcessor`) that let the consumer dispatch without reflection, with the generic ceremony (deserialization, response serialization) pushed into `StandardProcessor<T>`/`RpcProcessor<TReq,TRes>` abstract base classes. This is exactly the abstraction the docs describe, and it's implemented cleanly in ~20 lines each.
- **Defensive resource cleanup**: every `DisposeAsync`/`Dispose` method wraps close/dispose calls in `try { } catch { /* ignore */ }` — a deliberate, reasonable choice for best-effort teardown rather than an oversight (the pattern repeats consistently across `RabbitMqConnection`, `RabbitMqRpcSender`, `RpcRequestResponseHandler`).
- **`InternalsVisibleTo`** is used correctly to share `internal` implementation types across the `MqCSFramework` → `.Sender`/`.Consumer`/`.Tests` split without over-exposing public API surface.
- **`ArgumentException.ThrowIfNullOrWhiteSpace` / `ArgumentNullException.ThrowIfNull`** guard clauses on every public registration method (`AddMqSender`, `AddMqRpcSender`, `AddMqConsumer`) — modern .NET 8+ idiom, applied consistently.

### Confirmed weaknesses

- **`MqConsumer.cs` (13KB) concentrates a lot of responsibility**: header parsing orchestration, pattern dispatch, retry bookkeeping, DLX publishing, timeout-token construction, and body-logging all live in one internal class. It's readable and each method is short, but it's doing the work of what other frameworks in this space typically split into a dispatcher + a retry policy + a dead-letter strategy as separate, independently testable components. Given zero of this logic is currently under test (§9), the concentration matters more than it otherwise would.
- **Fail-open masking**: `LogMaskingHelper.Mask` returns the **original, unmasked** JSON string if `JsonNode.Parse` throws (e.g., the body isn't valid JSON). For a feature whose entire purpose is keeping sensitive fields out of logs, failing open rather than suppressing the log or the field is a specific, fixable design weakness.
- **`ConsumerHostedService.ExecuteAsync` has no per-consumer isolation [new — see §11]**: a plain `foreach` over registrations with no per-iteration `try/catch`, meaning one consumer's `StartAsync` failure can prevent every other configured consumer from starting and, depending on the host's `BackgroundServiceExceptionBehavior`, can terminate the entire process.
- **Package versioning defect [new]**: none of the three `.csproj` files (nor `Directory.Build.props`) set a `<Version>`/`<PackageVersion>`, and all three set `GeneratePackageOnBuild=true`. The local build artifacts confirm the result: `MqCSFramework.1.0.0.nupkg`, `MqCSFramework.Sender.1.0.0.nupkg`, `MqCSFramework.Consumer.1.0.0.nupkg` — a stable-looking `1.0.0` version number for software the project's own git tags and `CHANGELOG.md` call `v0.3.0-alpha`. This is a real release-hygiene defect: if these artifacts were ever pushed as-is, they'd misrepresent alpha software as a 1.0.0 release to anyone browsing by version.
- **Duplicated retry/DLX-publish construction logic** between the retry branch and the dead-letter branch in `HandleFailureAsync` (separately building near-identical `BasicProperties` from `ea.BasicProperties`) — minor, but a clear extraction candidate that hasn't happened yet.

### Net assessment

Materially better than the inferred version credited it for on craftsmanship, and with a shorter, more concrete list of real defects than a generic "unknown, alpha, be cautious" framing would suggest. The code reads like it was produced carefully against a detailed spec (consistent with the Kiro/spec-driven provenance in §2) rather than accreted ad hoc — which shows in the consistency, but doesn't by itself catch cross-cutting concerns like the persistence gap or the consumer-isolation gap, which are exactly the kind of thing a spec can state correctly (`requirements.md` §4.4: *"Connections auto-reconnect on failure"* — true, verified) while a different, unstated property (message durability under broker restart) goes unaddressed by both the spec and the code.

---

## 7. Repository Structure Audit **[corrected]**

Structure assessment is unchanged and positive (conventional `src`/`tests`/`samples`/`docs` layout, `Directory.Build.props`, `.slnx`). One correction from the original report:

- **`NuGet.Config` was speculated to possibly point at a private feed** — false. Read directly: it contains exactly one source, `nuget.org` (`https://api.nuget.org/v3/index.json`), with `<clear />` removing all others. There is no private feed. The straightforward explanation for the package's absence from nuget.org (confirmed by direct search in the original audit) is simply that **it has never been published** — `dotnet add package MqCSFramework`, as instructed in `README.md` and `docs/quickstart.md`, would fail for any person trying it today.
- **`specs/`** is now fully explained (§2): it's the Kiro-style spec-driven-development source of truth, not a mystery folder.

---

## 8. Documentation Quality Audit **[corrected — confirmed drift]**

The documentation is still the project's strongest area on prose quality and structural completeness — that assessment holds. What's new: direct comparison against source found **two confirmed, specific instances of docs lagging the shipped API**, which matters more than usual here given the project's own stated rule that documentation is the sole source of truth and must be updated *before* code.

1. **`SendAsync` signature mismatch.** `docs/api-reference.md`, `docs/quickstart.md`, and the root `README.md` all show:
   ```csharp
   Task<string> SendAsync<TProcessor, TMessage>(TMessage message, SendOptions? options = null, CancellationToken ct = default)
   ```
   The actual shipped interface (`src/MqCSFramework.Sender/IStandardSender.cs`, and identically `IRpcSender.cs`) is:
   ```csharp
   Task<string> SendAsync<TProcessor, TMessage>(TMessage message, string correlationId, SendOptions? options = null, CancellationToken ct = default)
   ```
   `correlationId` is a mandatory, no-default parameter (per `CHANGELOG.md` 0.2.0-alpha: *"CorrelationId is now a mandatory parameter on sender interfaces"*). **Copy-pasting the documented Quick Start example will not compile.** The actual sample code (`samples/MqCSFramework.Samples.Sender/Program.cs`) correctly passes `correlationId` — confirming the samples are current and it's specifically the prose docs that are stale.

2. **`ConsumerOptions` mismatch.** `docs/configuration.md` documents `SuppressMessageBodyLogging` (bool, default `false`) as a valid property. `CHANGELOG.md`'s own 0.2.0-alpha entry says it was *removed* ("body logging controlled via Serilog log levels"), and the real `ConsumerOptions.cs` confirms it doesn't exist. The same class has a real `ProcessingTimeoutMs` (int, default `30000`) property that **does not appear anywhere in `docs/configuration.md`**.

Both are small, mechanical fixes — but they're concrete evidence that the "documentation-first, single source of truth" process this project explicitly commits to (§2) has already lapsed twice in its first ~72 hours of existence. That's not a reason to distrust the docs wholesale (most of what's documented checked out exactly against source), but it means the docs should currently be read as "mostly current, verify signatures against source before depending on them" rather than authoritative.

---

## 9. Testing Audit **[confirmed — full test suite reviewed]**

Previously inferred as likely thin; now fully confirmed, with numbers.

- **The entire automated test suite is `tests/MqCSFramework.Tests/DiRegistrationTests.cs` — three `[Fact]` methods, all verifying only that a DI registration resolves a non-null service or a registration count:**
  - `AddMqSender_RegistersKeyedStandardSender` — asserts `GetKeyedService<IStandardSender>("test")` is not null.
  - `AddMqRpcSender_RegistersKeyedRpcSender` — asserts `GetKeyedService<IRpcSender>("test-rpc")` is not null.
  - `AddMqConsumer_RegistersConsumerRegistration` — asserts exactly one `ConsumerRegistration` was registered.

  No test constructs a message, publishes anything, exercises retry/DLQ logic, verifies masking, checks RPC correlation, or touches a real or fake broker.

- **`specs/design.md` explicitly enumerates ten formally-worded "Correctness Properties"** (its own term), each written in "for any X, the system SHALL Y" form — e.g., *Property 2: Serialization Round-Trip*, *Property 7: Dead-Letter Routing on Retry Exhaustion*, *Property 8: Sensitive Field Masking*, *Property 10: Cancellation Deadline Propagation (RPC)*. **Zero of these ten properties has a corresponding automated test.** This is the sharpest, most quantifiable version of the "no test strategy" concern raised in the original report: the project's own design document specifies exactly what should be tested, in exactly the language property-based tests are written in, and none of it is.
- CI (`.github/workflows/build.yml`) does run `dotnet test` on every push/PR to `master` — confirmed, and a genuine positive relative to typical week-old repos — but with only the three DI-resolution tests, CI currently validates "the container wires up" and nothing about message-handling correctness.
- `coverlet.collector` is referenced in the test `.csproj` (coverage tooling is present) but CI never invokes `--collect` or publishes a coverage report, so even the thin coverage that exists isn't measured or gated.

---

## 10. Security Audit

Findings from the original audit are confirmed and one item is now precise rather than speculative:

| Item | Severity | Status |
|---|---|---|
| Plaintext password in config schema | Medium | Confirmed (`RabbitMqConnectionOptions.Password` is a plain `string`; no secrets-manager hook). Mitigated by convention (`appsettings.local.json` gitignored), not by enforcement. |
| Consumer resolves processor type from an unsigned wire header | Medium | **[new, see §4]** `Type.GetType()` on the `mq-processor-type` header, bounded to already-DI-registered types but with no check that the payload came from a sender that was actually meant to target that processor. |
| No dependency vulnerability scanning | Low-Medium | **Confirmed, not just inferred** — `build.yml` runs restore/build/test only; no Dependabot config, no `dotnet list package --vulnerable` step, no CodeQL. |
| Input validation on deserialized messages | Medium | Confirmed absent — `StandardProcessor<T>`/`RpcProcessor<TReq,TRes>` deserialize-or-throw with no schema/validation hook. |
| Fail-open sensitive-field masking | **[new]** Medium | See §6 — malformed/non-JSON bodies are logged unmasked rather than suppressed. |
| Broker-level authN/authZ | Low (by design, correctly out of scope) | Unchanged — correctly deferred to RabbitMQ itself. |

---

## 11. DevOps and Deployment Audit **[corrected]**

Docker/Kubernetes/observability findings are unchanged (no Dockerfile or compose file for the sample services, no k8s manifests, no metrics/tracing/health-check surface anywhere in the reviewed source — confirmed by full read, not just absence-from-docs). One new, concrete finding:

- **No consumer-startup isolation.** `ConsumerHostedService.ExecuteAsync` starts every registered `MqConsumer` in a plain sequential `foreach` with no per-consumer exception boundary:
  ```csharp
  foreach (var reg in _registrations)
  {
      var consumer = new MqConsumer(reg.Options, _serviceProvider, _loggerFactory.CreateLogger<MqConsumer>());
      _consumers.Add(consumer);
      await consumer.StartAsync(stoppingToken);   // <-- unguarded
  }
  ```
  If any single consumer's `StartAsync` throws (wrong queue name, unreachable broker for that specific connection, auth failure), the exception propagates out of `BackgroundService.ExecuteAsync` unhandled. Depending on the host's configured `BackgroundServiceExceptionBehavior` (the .NET default is to stop the host), **a single misconfigured consumer can prevent every other consumer in the same process from starting, or take down an otherwise-healthy multi-consumer host.** For a framework whose stated selling point is running multiple independent consumers in one Generic Host (`docs/overview.md`'s "Multi-Tenant Processing" use case), this is a meaningful, fixable gap — the fix is a `try/catch` per registration with a log-and-continue (or log-and-fail-that-one) policy.

**Determination (unchanged):** not operationally viable for production as-is, now for slightly different and more concrete reasons than the original inferred version.

---

## 12. Open-Source Project Health Assessment

Unchanged from the original — metadata re-verified via the uploaded `.git` directory, which is consistent with what was seen on GitHub:

| Metric | Value |
|---|---|
| Stars / Forks / Watchers | 0 / 0 / 0 |
| Total commits | 17 |
| Contributors | 1 (gabollado) |
| Git tags | `v0.1.0-alpha` (2026-08-01), `v0.2.0-alpha` (2026-08-03), `v0.3.0-alpha` (2026-08-03) |
| Published NuGet package | Confirmed not published (see §7) |

One nuance: `v0.3.0-alpha` was tagged locally *after* the last `CHANGELOG.md` entry (which stops at `0.2.0-alpha`) — the changelog is not fully current with the tag history either, consistent with the documentation-drift pattern in §8.

---

## 13. Strengths **[updated]**

1. **Genuinely well-crafted async/concurrency code** — confirmed by direct read, not just docs: correct locking, correct cancellation-token linking and cleanup, correct `IAsyncDisposable` patterns throughout.
2. **A real, working reconnection story** via RabbitMQ.Client's `AutomaticRecoveryEnabled`/`TopologyRecoveryEnabled`, layered with a defensive self-healing channel check — this was wrongly flagged as missing in the prior version of this audit.
3. **Documentation quality and structural completeness**, with the caveat in §8 that two specific places have drifted from source.
4. **Real compile-time type safety** between sender and consumer via generic processor interfaces — confirmed working as documented in both the framework code and the samples.
5. **Correlation ID and structured logging (with field masking)** are first-class and mostly well-implemented (masking has one fail-open gap, §6).
6. **Consistent, modern .NET 10/C# 14 idiom** throughout — `Directory.Build.props`-enforced nullable reference types, primary constructors, collection expressions, guard-clause helpers.
7. **CI runs real `dotnet test` on every push/PR**, a genuine positive relative to typical week-old repos, even though what's tested is currently minimal (§9).

---

## 14. Weaknesses **[updated]**

1. **Messages are never marked persistent** — the single highest-severity, most concrete finding in this audit. Durable queues do not protect message contents across a broker restart when `DeliveryMode` is never set to persistent. (§4)
2. **The entire test suite is three DI-resolution smoke tests** against ten explicitly-specified correctness properties in the project's own design document. (§9)
3. **No consumer-startup isolation** — one misconfigured consumer can block or crash the whole host. (§11)
4. **Confirmed documentation/code drift in two places**, notable specifically because the project's contribution workflow states documentation must be updated before code, and it wasn't. (§8)
5. **Package versioning defect**: unset `<Version>` produces misleadingly-stable `1.0.0` nupkgs for alpha software. (§6)
6. **RPC correlationId collision risk** when callers reuse a correlationId across concurrent in-flight calls — undocumented. (§4)
7. **Narrow duplicate-delivery window** in the retry/DLX publish-then-ack sequence on process crash. (§4)
8. **Fail-open sensitive-field masking** on non-JSON or malformed message bodies. (§6, §10)
9. **Zero community validation** — 0 stars/forks, single contributor, ~2 weeks old, package never published. (§12)
10. **No observability** (metrics, tracing, health checks) anywhere in the reviewed source. (§11)
11. Only the default exchange is exercised anywhere in code, samples, or tests — the "event-driven architecture" use case remains aspirational. (§4)

---

## 15. Production Readiness Score: **2 / 10** *(unchanged)*

The reconnection correction (§4) and genuinely solid async/concurrency code (§6) are real positives that a source-blind audit couldn't credit. They're offset by newly *confirmed* — not merely inferred — issues of comparable or greater severity: message persistence is definitively off, the correctness-property test coverage is definitively zero, and the multi-consumer host has a definitively unguarded single point of failure. The net position doesn't move: this is unsuitable for production today, but the specific reasons are now concrete and, notably, mostly small, well-scoped fixes rather than fundamental design flaws.

## 16. Usefulness Score: **5 / 10** *(unchanged, higher confidence)*

The code-quality finding modestly strengthens the "good learning reference" case — this is a legitimately well-written example of a typed-messaging layer over RabbitMQ.Client, better than the alpha/solo-maintainer label would suggest. That doesn't change its position against mature, battle-tested alternatives (MassTransit, Rebus, EasyNetQ, NServiceBus) for real adoption, which is where most of this score's ceiling comes from.

---

## 17. Risk Matrix **[updated]**

| Risk | Likelihood | Impact | Severity |
|---|---|---|---|
| **Message loss on broker restart (no persistent delivery mode)** | **High** (any broker restart/crash while messages are queued) | High | **Critical** |
| Silent message loss via unconfirmed DLX/retry publish | Medium | High | High |
| Single consumer misconfiguration crashes/blocks the whole host | Medium | High | High |
| Undetected correctness regressions (zero coverage of 10 specified properties) | High | Medium-High | High |
| RPC correlationId collision under concurrent reuse | Low-Medium | Medium | Medium |
| Duplicate processing in narrow retry/DLX crash window | Low | Medium | Medium |
| Docs drifting further from a fast-moving pre-1.0 API | Medium | Medium | Medium |
| Sensitive data logged unmasked on malformed JSON bodies | Low | Medium | Low-Medium |
| Project abandonment (single maintainer, unpublished package) | Medium | High for any adopter | High |

The persistence gap is elevated to **Critical** here versus "High" in the original inferred risk matrix, because it's now a confirmed fact rather than an unconfirmed possibility, and it's a near-certainty to bite anyone running this against a broker that ever restarts.

---

## 18. Recommended Improvements (Prioritized) **[updated]**

1. **Set `Persistent = true` (`DeliveryMode = 2`) on published `BasicProperties`** in both senders. This is the single highest-value fix in the entire codebase relative to effort — roughly a two-line change.
2. **Wrap each consumer's `StartAsync` in `ConsumerHostedService.ExecuteAsync`** in its own try/catch with a clear log-and-continue (or explicit fail-fast, but *chosen* rather than accidental) policy.
3. **Write tests for the ten correctness properties already specified in `specs/design.md`** — they're written almost test-ready; this is the fastest path from "zero tests" to a real safety net, and could reasonably use an in-memory/fake channel or Testcontainers-backed RabbitMQ.
4. **Regenerate `docs/api-reference.md`, `docs/quickstart.md`, and the root `README.md`** against the current `IStandardSender`/`IRpcSender`/`ConsumerOptions` shapes — both confirmed mismatches are mechanical fixes.
5. **Set an explicit `<Version>`** in `Directory.Build.props` matching the git tag / CHANGELOG version, so local `dotnet pack` output stops silently producing `1.0.0`.
6. **Document that `correlationId` must be unique per in-flight RPC call on a given sender instance**, or generate it internally and return it alongside the response instead of accepting it from the caller.
7. **Add publisher confirms (or at minimum `mandatory: true` with a `BasicReturn` handler)** on the DLX and retry-republish paths so misrouted dead-letter/retry messages are detected rather than silently dropped.
8. **Change `LogMaskingHelper.Mask` to fail closed** — on JSON parse failure, suppress the body or log a placeholder rather than the raw unmasked string.
9. Publish the package to nuget.org, or update the README/quickstart to reflect the actual current installation method (project/source reference).
10. Add OpenTelemetry tracing and a basic `IHealthCheck` for connection state — still the largest structural gap for production operation.

---

## 19. Final Verdict

Unchanged: **Use only for learning.**

What changed is *why*. The original verdict rested on an unproven, under-documented project where the biggest visible risk was "we don't know how this behaves under failure." Having read the source, the honest picture is more specific and, in some ways, more encouraging: the failure-handling code exists, is mostly well-written, and gets several hard things (reconnection, RPC cancellation, correlation propagation) right. But it also has a small number of **concrete, confirmed defects** — message persistence chief among them — that are exactly the kind of thing a real test suite would have caught, and didn't, because the real test suite doesn't yet test any of the behavior the project's own design document says it must. That combination — solid mechanics, zero verification of the properties that matter, and a documentation process that's already fallen behind its own stated rule — is a more precise reason to wait for a hardened release than "it's new," and a more actionable one for the maintainer to fix.
