# Quick Start

Get MqCSFramework running in under 5 minutes.

## Prerequisites

- .NET 10 SDK
- A RabbitMQ instance (local or cloud — see [Setting Up RabbitMQ](#setting-up-rabbitmq) below)

## 1. Install the Package

```bash
dotnet add package MqCSFramework
```

## 2. Define Your Contracts

Create a shared class library referenced by both sender and consumer projects:

```csharp
// Messages
public record OrderMessage(Guid OrderId, string CustomerName, decimal Amount);

// Processor contract interface
public interface IOrderProcessor : IMessageProcessor<OrderMessage>;
```

For RPC (request-reply):

```csharp
public record StockRequest(string Sku, int Quantity);
public record StockResponse(bool Available, int RemainingStock);

public interface IStockProcessor : IRpcProcessor<StockRequest, StockResponse>;
```

## 3. Implement Your Processor

In your consumer project, inherit from the abstract base class:

```csharp
public class OrderProcessor : StandardProcessor<OrderMessage>, IOrderProcessor
{
    public override Task ProcessAsync(OrderMessage message, MessageContext context, CancellationToken ct = default)
    {
        Console.WriteLine($"Processing order {message.OrderId} for {message.CustomerName}");
        return Task.CompletedTask;
    }
}
```

For RPC:

```csharp
public class StockProcessor : RpcProcessor<StockRequest, StockResponse>, IStockProcessor
{
    public override Task<StockResponse> ProcessAsync(StockRequest request, MessageContext context, CancellationToken ct = default)
    {
        var response = new StockResponse(Available: true, RemainingStock: 42);
        return Task.FromResult(response);
    }
}
```

## 4. Configure the Consumer

```csharp
var builder = Host.CreateApplicationBuilder(args);

// Register processors as DI singletons
builder.Services.AddSingleton<IOrderProcessor, OrderProcessor>();
builder.Services.AddSingleton<IStockProcessor, StockProcessor>();

// One-line framework setup from appsettings.json
builder.Services.AddMqCSFramework(builder.Configuration);

await builder.Build().RunAsync();
```

## 5. Configure the Sender

```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddMqCSFramework(builder.Configuration);

var app = builder.Build();

// Send a standard message
var sender = app.Services.GetRequiredKeyedService<IStandardSender>("orders");
await sender.SendAsync<IOrderProcessor, OrderMessage>(
    new OrderMessage(Guid.NewGuid(), "Alice", 99.99m));

// Send an RPC request
var rpcSender = app.Services.GetRequiredKeyedService<IRpcSender>("stock");
var response = await rpcSender.SendAsync<IStockProcessor, StockResponse, StockRequest>(
    new StockRequest("SKU-123", 2));

Console.WriteLine($"Available: {response.Available}");
```

## 6. Add appsettings.json

```json
{
  "MqCSFramework": {
    "Senders": {
      "orders": {
        "Connection": {
          "HostName": "localhost",
          "Port": 5672,
          "UserName": "guest",
          "Password": "guest",
          "VirtualHost": "/"
        },
        "Exchange": "",
        "RoutingKey": "orders-queue"
      }
    },
    "RpcSenders": {
      "stock": {
        "Connection": {
          "HostName": "localhost",
          "Port": 5672,
          "UserName": "guest",
          "Password": "guest",
          "VirtualHost": "/"
        },
        "Exchange": "",
        "RoutingKey": "stock-queue",
        "Timeout": "00:00:30"
      }
    },
    "Consumers": {
      "orders": {
        "Connection": {
          "HostName": "localhost",
          "Port": 5672,
          "UserName": "guest",
          "Password": "guest",
          "VirtualHost": "/"
        },
        "QueueName": "orders-queue",
        "PrefetchCount": 10
      },
      "stock": {
        "Connection": {
          "HostName": "localhost",
          "Port": 5672,
          "UserName": "guest",
          "Password": "guest",
          "VirtualHost": "/"
        },
        "QueueName": "stock-queue",
        "PrefetchCount": 10
      }
    }
  }
}
```

## Running the Included Samples

The repository includes working sample projects you can run immediately:

```bash
# Clone the repository
git clone https://github.com/gabollado/MqCSFramework.git
cd MqCSFramework

# Start the consumer first (from its project directory)
cd samples/MqCSFramework.Samples.Consumer
dotnet run

# In another terminal, run the sender
cd samples/MqCSFramework.Samples.Sender
dotnet run
```

The consumer will print:
```
[Consumer] Processing order ... for Alice - Amount: 99,99 €
[Consumer] Checking stock for SKU SKU-12345, quantity 2
```

The sender will print:
```
[Sender] Order sent: <message-id>
[Sender] Stock check: Available=True, Remaining=42
```

> **Note:** The samples are configured to use a CloudAMQP instance. To use your own RabbitMQ, edit the `appsettings.json` in each sample project.

## Creating Your Own Example

1. Create a new solution with three projects:
   ```bash
   dotnet new sln -n MyMessagingApp
   dotnet new classlib -n MyApp.Contracts
   dotnet new console -n MyApp.Sender
   dotnet new console -n MyApp.Consumer
   ```

2. Add the MqCSFramework reference to Sender and Consumer projects:
   ```bash
   dotnet add MyApp.Sender package MqCSFramework
   dotnet add MyApp.Consumer package MqCSFramework
   ```

3. Add the Contracts project reference to both:
   ```bash
   dotnet add MyApp.Sender reference MyApp.Contracts
   dotnet add MyApp.Consumer reference MyApp.Contracts
   ```

4. Follow steps 2-6 above to define contracts, implement processors, and configure.

## Setting Up RabbitMQ

### Docker (local)

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:management
```

Management UI: http://localhost:15672 (guest/guest). The default `appsettings.json` in the samples already points to localhost.

### CloudAMQP (cloud, no Docker)

1. Create a free account at https://www.cloudamqp.com/
2. Create an instance, copy connection details
3. Put them in `appsettings.local.json` (git-ignored) with `"UseSsl": true` and `"Port": 5671`
