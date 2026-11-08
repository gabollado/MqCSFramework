# Configuration Reference

MqCSFramework reads all its configuration from `IConfiguration` (typically `appsettings.json`). This document details every available option.

## Registration Methods

### One-line setup (recommended)

```csharp
// Reads from "MqCSFramework" section automatically
builder.Services.AddMqCSFramework(builder.Configuration);
```

### Custom section name

```csharp
// Reads from a custom section name
builder.Services.AddMqCSFramework(builder.Configuration, "MyCustomSection");
```

### Manual builder (full control)

```csharp
builder.Services.AddMqCSFramework(mq =>
{
    mq.AddSender("orders", opts =>
    {
        opts.Connection.HostName = "localhost";
        opts.Exchange = "";
        opts.RoutingKey = "orders-queue";
    });

    mq.AddConsumer("orders", opts =>
    {
        opts.Connection.HostName = "localhost";
        opts.QueueName = "orders-queue";
    });
});
```

### Hybrid (config + manual)

```csharp
builder.Services.AddMqCSFramework(mq =>
{
    mq.BindConfiguration(builder.Configuration.GetSection("MqCSFramework"));
    // Add additional manual registrations here if needed
});
```

---

## appsettings.json Structure

```json
{
  "MqCSFramework": {
    "Senders": {
      "<name>": { /* StandardSenderOptions */ }
    },
    "RpcSenders": {
      "<name>": { /* RpcSenderOptions */ }
    },
    "Consumers": {
      "<name>": { /* ConsumerOptions */ }
    }
  }
}
```

The `<name>` key is used as the keyed service name for DI injection.

---

## RabbitMqConnectionOptions

Embedded in each sender/consumer. Every endpoint has its own independent connection.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `HostName` | string | `"localhost"` | One or more comma-separated `host` or `host:port` entries (the nodes of one cluster). An entry without a port uses 5672, or 5671 with `UseSsl`. With several entries the client tries them in random order on connect and on recovery |
| `UserName` | string | `"guest"` | Authentication username |
| `Password` | string | `"guest"` | Authentication password |
| `VirtualHost` | string | `"/"` | RabbitMQ virtual host |
| `UseSsl` | bool | `false` | Enable SSL/TLS for every host |
| `SslServerName` | string? | `null` | Certificate name checked for every host. When empty, each host is checked against its own name |
| `ClientProvidedName` | string? | `null` | Connection name visible in RabbitMQ management UI |
| `RequestedConnectionTimeout` | TimeSpan | `00:00:30` | Budget per host for opening a connection (TCP, TLS, AMQP handshake). The whole connect fails with `TimeoutException` after this times the number of hosts |

There is no separate `Port` setting: put a non-default port in `HostName` (`"rabbit.example.com:5673"`).

### Example

```json
"Connection": {
  "HostName": "rabbit.example.com",
  "UserName": "myapp",
  "Password": "secret",
  "VirtualHost": "/production",
  "UseSsl": true,
  "ClientProvidedName": "order-service-sender"
}
```

### Cluster example

```json
"Connection": {
  "HostName": "rabbit-1.example.com:5671,rabbit-2.example.com:5671,rabbit-3.example.com:5671",
  "UseSsl": true
}
```

---

## StandardSenderOptions

Configuration for a fire-and-forget sender.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `Connection` | RabbitMqConnectionOptions | (see above) | Connection settings for this sender |
| `Exchange` | string | `""` | Exchange to publish to (empty = default exchange) |
| `RoutingKey` | string | `""` | Default routing key (queue name for default exchange) |
| `TimeoutMs` | int | `20000` | Upper bound for one send (connect if needed + publish), together with the caller's token. On expiry `SendAsync` throws `TimeoutException`. `0` disables it |

### Example

```json
"Senders": {
  "orders": {
    "Connection": { "HostName": "localhost" },
    "Exchange": "",
    "RoutingKey": "orders-queue"
  }
}
```

---

## RpcSenderOptions

Configuration for a request-reply sender.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `Connection` | RabbitMqConnectionOptions | (see above) | Connection settings for this sender |
| `Exchange` | string | `""` | Exchange to publish to |
| `RoutingKey` | string | `""` | Default routing key |
| `Timeout` | TimeSpan | `00:00:30` | How long to wait for a response before throwing `RpcTimeoutException` |

### Example

```json
"RpcSenders": {
  "stock": {
    "Connection": { "HostName": "localhost" },
    "Exchange": "",
    "RoutingKey": "stock-queue",
    "Timeout": "00:00:10"
  }
}
```

---

## ConsumerOptions

Configuration for a message consumer.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `Connection` | RabbitMqConnectionOptions | (see above) | Connection settings for this consumer |
| `QueueName` | string | `""` | Queue to consume from (declared automatically on startup) |
| `PrefetchCount` | ushort | `10` | Number of unacknowledged messages allowed (controls parallelism) |
| `MaxRetries` | int | `3` | Maximum retry attempts before dead-lettering (0 = NACK without requeue on first failure) |
| `DeadLetterExchange` | string? | `null` | Exchange to publish failed messages to after retry exhaustion |
| `DeadLetterRoutingKey` | string? | `null` | Routing key for dead-letter messages |
| `SuppressMessageBodyLogging` | bool | `false` | When true, message bodies are not included in log output |
| `MaskedFields` | string[] | `[]` | JSON field names whose values are replaced with `***MASKED***` in logs |

### Example

```json
"Consumers": {
  "orders": {
    "Connection": {
      "HostName": "rabbit.example.com",
      "UserName": "consumer",
      "Password": "secret",
      "VirtualHost": "/production",
      "UseSsl": true,
      "ClientProvidedName": "order-consumer"
    },
    "QueueName": "orders-queue",
    "PrefetchCount": 20,
    "MaxRetries": 5,
    "DeadLetterExchange": "orders-dlx",
    "DeadLetterRoutingKey": "orders.dead",
    "SuppressMessageBodyLogging": false,
    "MaskedFields": ["password", "creditCard", "token"]
  }
}
```

---

## Per-Message Options

These can be passed at send time to override sender defaults.

### SendOptions (for IStandardSender)

| Property | Type | Description |
|----------|------|-------------|
| `RoutingKey` | string? | Override the sender's default routing key |
| `CorrelationId` | string? | Set a specific correlation ID (auto-generated if null) |
| `AdditionalHeaders` | Dictionary? | Extra headers to include on the message |

### RpcOptions (for IRpcSender)

| Property | Type | Description |
|----------|------|-------------|
| `RoutingKey` | string? | Override the sender's default routing key |
| `CorrelationId` | string? | Set a specific correlation ID |
| `Timeout` | TimeSpan? | Override the sender's default timeout |
| `AdditionalHeaders` | Dictionary? | Extra headers to include on the message |

### Usage

```csharp
await sender.SendAsync<IOrderProcessor, OrderMessage>(
    new OrderMessage(Guid.NewGuid(), "Bob", 49.99m),
    new SendOptions { CorrelationId = "my-custom-id" });
```

---

## Logging Configuration (Serilog)

MqCSFramework uses `ILogger` from Microsoft.Extensions.Logging. We recommend Serilog for structured file + console logging.

### Required Packages

```bash
dotnet add package Serilog.Extensions.Hosting
dotnet add package Serilog.Sinks.Console
dotnet add package Serilog.Sinks.File
dotnet add package Serilog.Settings.Configuration
```

### Setup in Program.cs

```csharp
builder.Services.AddSerilog(config => config.ReadFrom.Configuration(builder.Configuration));
```

### appsettings.json Serilog Section

```json
{
  "Serilog": {
    "Using": ["Serilog.Sinks.Console", "Serilog.Sinks.File"],
    "MinimumLevel": "Information",
    "WriteTo": [
      { "Name": "Console" },
      {
        "Name": "File",
        "Args": {
          "path": "C:\\Logging\\myapp-.log",
          "rollingInterval": "Day"
        }
      }
    ]
  }
}
```

---

## Multiple Brokers

Each sender/consumer has its own connection. You can connect to different RabbitMQ clusters from one service:

```json
{
  "MqCSFramework": {
    "Senders": {
      "orders": {
        "Connection": { "HostName": "rabbit-cluster-a.internal" },
        "RoutingKey": "orders-queue"
      }
    },
    "RpcSenders": {
      "inventory": {
        "Connection": { "HostName": "rabbit-cluster-b.internal" },
        "RoutingKey": "inventory-queue"
      }
    }
  }
}
```
