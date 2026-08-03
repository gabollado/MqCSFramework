# Samples

## Included Samples

The repository includes three sample projects demonstrating the complete workflow:

```
samples/
├── MqCSFramework.Samples.Contracts/   ← Shared message types + processor interfaces
├── MqCSFramework.Samples.Sender/      ← Sends standard + RPC messages
└── MqCSFramework.Samples.Consumer/    ← Processes messages from queues
```

### Project Structure

- **Contracts** — A class library containing the message records (`OrderMessage`, `StockRequest`, `StockResponse`) and processor contract interfaces (`IOrderProcessor`, `IStockProcessor`). Referenced by both sender and consumer. Only references `MqCSFramework`.

  Key types:
  - `public record OrderMessage(Guid OrderId, string CustomerName, decimal Amount, DateTimeOffset CreatedAt)`
  - `public record StockRequest(string Sku, int Quantity)`
  - `public record StockResponse(bool Available, int RemainingStock, decimal UnitPrice)`
  - `public interface IOrderProcessor : IMessageProcessor<OrderMessage>`
  - `public interface IStockProcessor : IRpcProcessor<StockRequest, StockResponse>`

- **Consumer** — A Generic Host console app that:
  - Configures Serilog from `appsettings.json`
  - Registers processor implementations as DI singletons (`AddSingleton<IOrderProcessor, OrderProcessor>()`)
  - Calls `AddMqCSFramework(builder.Configuration)` to bind all consumers from config
  - Processor implementations inherit from `StandardProcessor<T>` or `RpcProcessor<TReq, TRes>`, implement the contract interface, inject `ILogger<T>` via primary constructor, and log all received context values and message properties

- **Sender** — A console app that:
  - Configures Serilog from `appsettings.json`
  - Calls `AddMqCSFramework(builder.Configuration)` to bind all senders from config
  - Resolves `IStandardSender` and `IRpcSender` via keyed DI (`GetRequiredKeyedService`)
  - Each request creates its own `CancellationTokenSource` with a 10-second timeout
  - Sends a standard `OrderMessage` and an RPC `StockRequest`, logging all values and the response

### Configuration

Both sender and consumer read connection details from `appsettings.json` (format documented in [Configuration Reference](configuration.md)). The key specifics for the samples:

- **Consumer queues:** `orders-queue` (standard, prefetch 20, max retries 3) and `stock-queue` (RPC, prefetch 10)
- **Sender routing keys:** `orders-queue` (standard) and `stock-queue` (RPC, timeout 10s)
- **Serilog:** Console + File sink to `C:\Logging\`
- **Connection credentials:** Stored in `appsettings.local.json` (git-ignored) — copy `appsettings.json` and fill in your connection details

### Package References (samples)

Both sender and consumer reference:
- `MqCSFramework` (the framework itself)
- `MqCSFramework.Samples.Contracts` (shared messages + interfaces)
- `Microsoft.Extensions.Hosting`
- `Serilog.Extensions.Hosting`
- `Serilog.Sinks.Console`
- `Serilog.Sinks.File`
- `Serilog.Settings.Configuration`

### Running the Samples

**1. Start the consumer:**

```bash
cd samples/MqCSFramework.Samples.Consumer
dotnet run
```

The consumer will connect to RabbitMQ, declare the queues, and start listening.

**2. In another terminal, run the sender:**

```bash
cd samples/MqCSFramework.Samples.Sender
dotnet run
```

The sender will publish a standard order message and an RPC stock request. You'll see the full round-trip in both terminals with message IDs, correlation IDs, timestamps, and all message property values.

---

## Creating Your Own Example

### 1. Create the solution structure

Three projects: a shared contracts library, a sender console app, and a consumer console app. Both sender and consumer reference the contracts project and the MqCSFramework package.

### 2. Define contracts

In the contracts project, define:
- Message records (the data you're sending)
- Processor contract interfaces inheriting from `IMessageProcessor<TMessage>` or `IRpcProcessor<TRequest, TResponse>`

### 3. Implement processors

In the consumer project, create classes that:
- Inherit from `StandardProcessor<TMessage>` (for fire-and-forget) or `RpcProcessor<TRequest, TResponse>` (for RPC)
- Implement the processor contract interface
- Accept `ILogger<T>` via primary constructor for structured logging
- Override `ProcessAsync` with your business logic

### 4. Register and configure

- Register processors as DI singletons: `services.AddSingleton<IMyProcessor, MyProcessorImpl>()`
- Add framework: `services.AddMqCSFramework(configuration)`
- Add Serilog: `services.AddSerilog(config => config.ReadFrom.Configuration(configuration))`
- Provide `appsettings.json` with Serilog and MqCSFramework sections

### 5. Send messages

In the sender, resolve `IStandardSender` or `IRpcSender` via keyed DI and call `SendAsync` with the processor interface as the generic parameter.

---

## Setting Up RabbitMQ

### Option 1: Docker (local development)

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:management
```

Management UI: http://localhost:15672 (guest/guest)

### Option 2: CloudAMQP (free tier, no Docker needed)

1. Go to https://www.cloudamqp.com/ and create a free account
2. Create a new instance (free "Little Lemur" plan)
3. Copy the connection details (host, user, password, vhost) into your `appsettings.local.json`
4. Use port 5671 with `UseSsl: true` for cloud instances

### Queue Configuration

The framework declares queues automatically on consumer startup (durable, non-exclusive, non-auto-delete). No manual queue creation is needed for basic usage.

For RPC, the sender creates an exclusive auto-delete reply queue automatically.
