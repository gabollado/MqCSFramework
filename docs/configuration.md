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

### Unknown keys

When senders and consumers are registered from configuration, every key under their section that matches no setting (case-insensitive, including inside `Connection`) is logged as a **warning** with its full path, e.g. `MqCSFramework:Senders:orders:Connection:Port`. Sender warnings appear when the sender is first created, consumer warnings when the consumer starts.

The key is still ignored, so a key you disabled on purpose (for example by renaming it) doesn't stop the application. Watch for these warnings after upgrading: a setting that was renamed or removed shows up there, such as the old `Port` (ports now go in `HostName`).

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
| `ConfirmPublish` | bool | `false` | Wait for the broker to accept every publish and fail when it doesn't. **Set it to `true` for standard senders**; optional for RPC senders. See [Publisher confirms](#publisher-confirms-confirmpublish) |

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

## Publisher confirms (`ConfirmPublish`)

> **Recommendation:** set `"ConfirmPublish": true` on the `Connection` of every **standard sender**. With the default `false`, `SendAsync` can succeed for a message that the broker never stored. For **RPC senders** it is optional.

`ConfirmPublish` lives on `Connection`, so it applies to everything that publishes through that connection: standard senders, RPC senders, and consumers (their RPC replies, retry republishes and dead-letter publishes). The default is `false`.

| | `false` (default) | `true` |
|---|---|---|
| `SendAsync` completes when | the message is written to the network socket | the broker has accepted the message |
| No queue matches the exchange/routing key (typo, queue not declared yet, missing binding) | message silently dropped, `SendAsync` still succeeds | throws `PublishReturnException` |
| The broker rejects the message (e.g. a queue length limit with `overflow: reject-publish`) | message lost, `SendAsync` still succeeds | throws `PublishException` |
| Cost per publish | none | waits for the broker's ack: about a millisecond locally, up to a few hundred milliseconds under load, because messages are persistent and the broker acks only after writing them to disk (or replicating them, for quorum queues) |

### Which side needs it

- **Standard senders — turn it on.** A fire-and-forget send has no other way to find out the message was lost.
- **RPC senders — optional.** A request that never reaches a queue already ends as `RpcTimeoutException`, because no reply comes back. With `true` it fails at once with `PublishException` instead of after the full timeout, and it is not retried (no consumer could answer it).
- **Consumers — recommended when you use retries or a dead-letter exchange.** With `false`, a retry or dead-letter copy that the broker drops is lost, and the original message is still acknowledged. With `true`, the failure is logged and the original is NACKed instead. An RPC reply that can't be delivered (the sender is gone) is only logged; the request is not processed again.

### Handling the exceptions

`PublishReturnException` derives from `PublishException`; both are in `RabbitMQ.Client.Exceptions` and carry details of what the broker said (`PublishReturnException` has the exchange, routing key and reply code). The connection and channel stay open, so after fixing the routing you can simply send again.

```csharp
try
{
    await sender.SendAsync<IOrderProcessor, OrderMessage>(order, correlationId);
}
catch (PublishReturnException ex)
{
    // No queue matched: check the exchange, routing key and bindings (ex.Message has the details)
}
catch (PublishException)
{
    // The broker rejected the message
}
```

### Example

```json
"Senders": {
  "orders": {
    "Connection": { "HostName": "localhost", "ConfirmPublish": true },
    "Exchange": "",
    "RoutingKey": "orders-queue"
  }
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

Set `Connection.ConfirmPublish` to `true` for standard senders, otherwise a message that can't be routed or is rejected is lost without an error (see [Publisher confirms](#publisher-confirms-confirmpublish)).

### Example

```json
"Senders": {
  "orders": {
    "Connection": { "HostName": "localhost", "ConfirmPublish": true },
    "Exchange": "",
    "RoutingKey": "orders-queue"
  }
}
```

---

## RpcSenderOptions

Configuration for a request-reply sender. `Connection.ConfirmPublish` is optional here: a lost request already ends as `RpcTimeoutException`; with `true` it fails at once with `PublishException` instead (see [Publisher confirms](#publisher-confirms-confirmpublish)).

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

Configuration for a message consumer. If you use retries or a dead-letter exchange, set `Connection.ConfirmPublish` to `true` so a copy the broker doesn't accept is never acknowledged away (see [Publisher confirms](#publisher-confirms-confirmpublish)).

If a consumer can't start (for example its broker is down, or the connect times out), the error is logged and the start is retried every 10 seconds until it works or the application stops. The application keeps running, and the other consumers start independently.

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
