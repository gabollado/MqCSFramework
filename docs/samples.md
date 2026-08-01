# Samples

## Included Samples

The repository includes three sample projects demonstrating the complete workflow:

```
samples/
├── MqCSFramework.Samples.Contracts/   ← Shared message types + processor interfaces
├── MqCSFramework.Samples.Sender/      ← Sends standard + RPC messages
└── MqCSFramework.Samples.Consumer/    ← Processes messages from queues
```

### Running the Samples

**1. Start the consumer:**

```bash
cd samples/MqCSFramework.Samples.Consumer
dotnet run
```

Expected output:
```
[Consumer] Starting...
[INF] Starting 2 consumer(s)
[INF] Consumer started on queue 'orders-queue' with prefetch 20
[INF] Consumer started on queue 'stock-queue' with prefetch 10
[INF] All consumers started
```

**2. In another terminal, run the sender:**

```bash
cd samples/MqCSFramework.Samples.Sender
dotnet run
```

Expected output:
```
[INF] Published standard message <id> for processor IOrderProcessor to /orders-queue
[Sender] Order sent: <id>
[INF] Published RPC request <id> for processor IStockProcessor to /stock-queue
[Sender] Stock check: Available=True, Remaining=42
```

**3. Back in the consumer terminal, you'll see:**

```
[Consumer] Processing order <id> for Alice - Amount: 99,99 €
[INF] Message <id> processed successfully by IOrderProcessor. ACK.
[Consumer] Checking stock for SKU SKU-12345, quantity 2
```

### Sample Structure

#### Contracts (shared between sender and consumer)

```csharp
// Messages
public record OrderMessage(Guid OrderId, string CustomerName, decimal Amount, DateTimeOffset CreatedAt);
public record StockRequest(string Sku, int Quantity);
public record StockResponse(bool Available, int RemainingStock, decimal UnitPrice);

// Processor interfaces
public interface IOrderProcessor : IMessageProcessor<OrderMessage>;
public interface IStockProcessor : IRpcProcessor<StockRequest, StockResponse>;
```

#### Consumer processors

```csharp
public class OrderProcessor : StandardProcessor<OrderMessage>, IOrderProcessor
{
    public override Task ProcessAsync(OrderMessage message, MessageContext context, CancellationToken ct = default)
    {
        Console.WriteLine($"Processing order {message.OrderId} for {message.CustomerName}");
        return Task.CompletedTask;
    }
}

public class StockProcessor : RpcProcessor<StockRequest, StockResponse>, IStockProcessor
{
    public override Task<StockResponse> ProcessAsync(StockRequest request, MessageContext context, CancellationToken ct = default)
    {
        return Task.FromResult(new StockResponse(Available: true, RemainingStock: 42, UnitPrice: 19.99m));
    }
}
```

#### Consumer Program.cs

```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSerilog(config => config.ReadFrom.Configuration(builder.Configuration));
builder.Services.AddSingleton<IOrderProcessor, OrderProcessor>();
builder.Services.AddSingleton<IStockProcessor, StockProcessor>();
builder.Services.AddMqCSFramework(builder.Configuration);

await builder.Build().RunAsync();
```

#### Sender Program.cs

```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSerilog(config => config.ReadFrom.Configuration(builder.Configuration));
builder.Services.AddMqCSFramework(builder.Configuration);

var app = builder.Build();

var sender = app.Services.GetRequiredKeyedService<IStandardSender>("orders");
await sender.SendAsync<IOrderProcessor, OrderMessage>(new OrderMessage(...));

var rpcSender = app.Services.GetRequiredKeyedService<IRpcSender>("stock");
var response = await rpcSender.SendAsync<IStockProcessor, StockResponse, StockRequest>(new StockRequest(...));
```

---

## Creating Your Own Example

### Step 1: Create the solution

```bash
mkdir MyMessagingApp && cd MyMessagingApp
dotnet new sln
dotnet new classlib -n MyApp.Contracts
dotnet new console -n MyApp.Sender
dotnet new console -n MyApp.Consumer
dotnet sln add MyApp.Contracts MyApp.Sender MyApp.Consumer
```

### Step 2: Add references

```bash
# Both sender and consumer reference contracts
dotnet add MyApp.Sender reference MyApp.Contracts
dotnet add MyApp.Consumer reference MyApp.Contracts

# Both reference MqCSFramework
dotnet add MyApp.Sender reference path/to/MqCSFramework.csproj
dotnet add MyApp.Consumer reference path/to/MqCSFramework.csproj

# Add Serilog to both
dotnet add MyApp.Sender package Serilog.Extensions.Hosting
dotnet add MyApp.Sender package Serilog.Sinks.Console
dotnet add MyApp.Sender package Serilog.Sinks.File
dotnet add MyApp.Sender package Serilog.Settings.Configuration
dotnet add MyApp.Consumer package Serilog.Extensions.Hosting
dotnet add MyApp.Consumer package Serilog.Sinks.Console
dotnet add MyApp.Consumer package Serilog.Sinks.File
dotnet add MyApp.Consumer package Serilog.Settings.Configuration
```

### Step 3: Define your messages and processor interface

In `MyApp.Contracts`:

```csharp
public record EmailRequest(string To, string Subject, string Body);

public interface IEmailProcessor : IMessageProcessor<EmailRequest>;
```

### Step 4: Implement the processor

In `MyApp.Consumer`:

```csharp
public class EmailProcessor : StandardProcessor<EmailRequest>, IEmailProcessor
{
    public override Task ProcessAsync(EmailRequest message, MessageContext context, CancellationToken ct = default)
    {
        Console.WriteLine($"Sending email to {message.To}: {message.Subject}");
        // Your email sending logic here
        return Task.CompletedTask;
    }
}
```

### Step 5: Wire up

Consumer `Program.cs`:
```csharp
var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSerilog(config => config.ReadFrom.Configuration(builder.Configuration));
builder.Services.AddSingleton<IEmailProcessor, EmailProcessor>();
builder.Services.AddMqCSFramework(builder.Configuration);
await builder.Build().RunAsync();
```

Sender `Program.cs`:
```csharp
var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSerilog(config => config.ReadFrom.Configuration(builder.Configuration));
builder.Services.AddMqCSFramework(builder.Configuration);
var app = builder.Build();

var sender = app.Services.GetRequiredKeyedService<IStandardSender>("email");
await sender.SendAsync<IEmailProcessor, EmailRequest>(
    new EmailRequest("user@example.com", "Welcome!", "Hello from MqCSFramework"));
```

### Step 6: Add appsettings.json to both projects

```json
{
  "Serilog": {
    "Using": ["Serilog.Sinks.Console"],
    "MinimumLevel": "Information",
    "WriteTo": [{ "Name": "Console" }]
  },
  "MqCSFramework": {
    "Senders": {
      "email": {
        "Connection": { "HostName": "localhost" },
        "Exchange": "",
        "RoutingKey": "email-queue"
      }
    },
    "Consumers": {
      "email": {
        "Connection": { "HostName": "localhost" },
        "QueueName": "email-queue"
      }
    }
  }
}
```

---

## Setting Up RabbitMQ

<!-- TODO: This section will be completed later with:
     - Running RabbitMQ locally with Docker
     - Setting up a free CloudAMQP instance
     - Creating queues, exchanges, and bindings for the examples
     - Management UI walkthrough
-->
