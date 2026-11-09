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

`ConfirmPublish` lives on `Connection`, so it applies to everything that publishes through that connection: standard senders, RPC senders, and consumers (their RPC replies). The default is `false`.

| | `false` (default) | `true` |
|---|---|---|
| `SendAsync` completes when | the message is written to the network socket | the broker has accepted the message |
| No queue matches the exchange/routing key (typo, queue not declared yet, missing binding) | message silently dropped, `SendAsync` still succeeds | throws `PublishReturnException` |
| The broker rejects the message (e.g. a queue length limit with `overflow: reject-publish`) | message lost, `SendAsync` still succeeds | throws `PublishException` |
| Cost per publish | none | waits for the broker's ack: about a millisecond locally, up to a few hundred milliseconds under load, because messages are persistent and the broker acks only after writing them to disk (or replicating them, for quorum queues) |

### Which side needs it

- **Standard senders — turn it on.** A fire-and-forget send has no other way to find out the message was lost.
- **RPC senders — optional.** A request that never reaches a queue already ends as `RpcTimeoutException`, because no reply comes back. With `true` it fails at once with `PublishException` instead of after the full timeout, and it is not retried (no consumer could answer it).
- **Consumers — not needed.** Retries and dead-lettering happen inside RabbitMQ, so the consumer publishes nothing but RPC replies. With `true`, an RPC reply that can't be delivered (the sender is gone) is logged; the request is not processed again.

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

Configuration for a message consumer. On startup the consumer uses the queue as it is if it already exists (classic or quorum, any arguments); otherwise it creates it as a quorum queue. Failed messages are dead-lettered by default; RabbitMQ can retry them instead (see [Retries and dead-lettering](#retries-and-dead-lettering)).

If a consumer can't start (for example its broker is down, or the connect times out), the error is logged and the start is retried every 10 seconds until it works or the application stops. The application keeps running, and the other consumers start independently.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `Connection` | RabbitMqConnectionOptions | (see above) | Connection settings for this consumer |
| `QueueName` | string | `""` | Queue to consume from (declared automatically on startup) |
| `PrefetchCount` | ushort | `10` | Number of unacknowledged messages allowed (controls parallelism) |
| `SuppressMessageBodyLogging` | bool | `false` | When true, message bodies are not included in log output |
| `MaskedFields` | string[] | `[]` | JSON field names whose values are replaced with `***MASKED***` in logs |
| `Retry` | bool | `false` | `false`: a failed message is dead-lettered (or dropped if the queue has no dead-letter exchange). `true`: RabbitMQ retries it. **Quorum queues only** (see below) |

The delay, retry limit and dead-letter exchange are not consumer settings; they live in RabbitMQ (a policy, or the queue's own arguments).

### Retries and dead-lettering

When a processor throws, the consumer rejects the message. `Retry` decides whether it goes back to the queue:

| | `Retry = false` (default) | `Retry = true` |
|---|---|---|
| Quorum queue | Dead-lettered on the first failure | RabbitMQ retries it with the delay and limit from a policy, then dead-letters it (RabbitMQ 4.3+) |
| Classic queue | Dead-lettered on the first failure | ⚠️ **Redelivered immediately and forever.** Classic queues have no delayed retry and no delivery limit. Don't use `Retry = true` on a classic queue |

"Dead-lettered" means sent to the queue's dead-letter exchange. **If the queue has none, the message is dropped**, so set a `dead-letter-exchange` (policy or queue argument) on every consumer queue. A message that can never be processed (missing headers, unknown processor) is always dead-lettered at once, whatever `Retry` is.

The consumer can't see the queue type (RabbitMQ doesn't report it to a client), so it's up to you to set `Retry` to match the queue.

Requirements and caveats for `Retry = true`:

- **RabbitMQ 4.3 or later.** Delayed retry exists only on quorum queues from 4.3.
- **Quorum queue.** A queue the consumer creates is always quorum. A queue that already exists keeps its type; to change a classic queue to quorum, delete it and let the consumer recreate it (step 2 below).
- **Set a policy.** Without one, a failed message is redelivered immediately until it has failed more than 20 times (RabbitMQ's default delivery limit), and then it is dropped.
- **Set a dead-letter exchange in the policy** if you don't want messages over the limit dropped. If that exchange doesn't exist, RabbitMQ drops them silently.

#### RabbitMQ setup

For `Retry = false` only step 3 and a policy with `dead-letter-exchange` are needed (classic queues accept it too: use `--apply-to queues`).

The example below uses the consumer queue `orders-queue` on vhost `/` with `"Retry": true`. It retries after 1s, 2s, 3s… capped at 30s. After more than 10 failures the message goes to the queue `orders-queue.dead`.

**1. Check the version.** The management UI shows it at the top of every page, or run `rabbitmqctl version`. It must be 4.3 or later.

**2. Remove the old queue if it is classic.** Skip this for a new queue. In the management UI, open *Queues and Streams*: the *Type* column must say `quorum`. If it says `classic`, stop the consumers, move or drain any messages you need, then delete the queue (queue page → *Delete*). The consumer recreates it as quorum on startup. Then set `"Retry": true` on the consumer.

**3. Create the dead-letter exchange and queue.** A `fanout` exchange sends every dead-lettered message to its queue whatever its routing key is.

Management UI:

- *Exchanges* → *Add a new exchange*: name `orders-dlx`, type `fanout`, durability `Durable`.
- *Queues and Streams* → *Add a new queue*: type `Quorum`, name `orders-queue.dead`.
- Open exchange `orders-dlx` → *Bindings* → *Add binding from this exchange*: to queue `orders-queue.dead`, routing key empty.

HTTP API (`%2F` is the vhost `/`):

```bash
curl -u admin:password -X PUT -H "content-type: application/json" \
  -d '{"type": "fanout", "durable": true}' \
  https://rabbit.example.com/api/exchanges/%2F/orders-dlx

curl -u admin:password -X PUT -H "content-type: application/json" \
  -d '{"durable": true, "arguments": {"x-queue-type": "quorum"}}' \
  https://rabbit.example.com/api/queues/%2F/orders-queue.dead

curl -u admin:password -X POST -H "content-type: application/json" \
  -d '{"routing_key": ""}' \
  https://rabbit.example.com/api/bindings/%2F/e/orders-dlx/q/orders-queue.dead
```

**4. Create the retry policy.** Use a name pattern that matches only the consumer queues. Here `^orders-queue$` doesn't match the dead-letter queue `orders-queue.dead`, so failed messages there are not retried.

| Key | Value | Meaning |
|-----|-------|---------|
| `delayed-retry-type` | `failed` | Delay messages whose delivery count went up. That covers the framework's reject, and also a consumer whose connection drops before it ACKs |
| `delayed-retry-min` | `1000` | Delay in ms after the first failure. The delay is `min × delivery count` |
| `delayed-retry-max` | `30000` | Maximum delay in ms |
| `delivery-limit` | `10` | Once a message has failed more times than this, it is dead-lettered (or dropped). Default 20 |
| `dead-letter-exchange` | `orders-dlx` | Where messages over the limit go |

rabbitmqctl (bash):

```bash
rabbitmqctl set_policy --vhost "/" orders-retry "^orders-queue$" \
  '{"delayed-retry-type": "failed", "delayed-retry-min": 1000, "delayed-retry-max": 30000, "delivery-limit": 10, "dead-letter-exchange": "orders-dlx"}' \
  --priority 10 --apply-to quorum_queues
```

Management UI: *Admin* → *Policies* → *Add / update a policy*. Enter name `orders-retry`, pattern `^orders-queue$`, *Apply to* `Queues [Quorum]`, priority `10`. Add each key from the table to *Definition*, setting the numeric values to type *Number*.

HTTP API:

```bash
curl -u admin:password -X PUT -H "content-type: application/json" \
  -d '{"pattern": "^orders-queue$", "apply-to": "quorum_queues", "priority": 10, "definition": {"delayed-retry-type": "failed", "delayed-retry-min": 1000, "delayed-retry-max": 30000, "delivery-limit": 10, "dead-letter-exchange": "orders-dlx"}}' \
  https://rabbit.example.com/api/policies/%2F/orders-retry
```

Policies apply right away to queues that already exist and to queues created later, so you can create the policy before or after the consumer first starts.

**5. Check it.** Start the consumer, then open the queue `orders-queue` in the management UI. *Type* should be `quorum`, *Policy* should be `orders-retry`, and *Effective policy definition* should list the keys above. If *Policy* shows a different policy, another policy matches the name with a higher priority. Only one policy applies to a queue, so raise the priority of yours (see the Amazon MQ note below).

Watch out for:

- **One policy per queue.** If a queue also needs other policy keys (`max-length`, `overflow`, …), put them in the same policy.
- **Amazon MQ.** You can't use `rabbitmqctl`. Use the RabbitMQ web console (Amazon MQ console → broker → *RabbitMQ web console URL*) or the HTTP API with an administrator user. On cluster brokers, Amazon's default `AWS-DEFAULT-QUORUM-QUEUES-POLICY-CLUSTER-MULTI-AZ` applies to quorum queues with priority 1. Give your policy a priority above 1 and copy that policy's definition keys into yours, otherwise your queue loses them. Delayed retry needs Amazon MQ for RabbitMQ 4.3, which is currently only on m7g instances.
- **Dead-lettering can lose messages by default.** A quorum queue dead-letters at-most-once by default, so a message can be lost if the dead-letter queue is unavailable when it is moved. For at-least-once, add `"dead-letter-strategy": "at-least-once"` and `"overflow": "reject-publish"` to the policy. RabbitMQ also recommends `max-length` with it. See [quorum queue dead-lettering](https://www.rabbitmq.com/docs/quorum-queues#dead-lettering).

References: [quorum queues: delayed retry and poison message handling](https://www.rabbitmq.com/docs/quorum-queues), [policies](https://www.rabbitmq.com/docs/policies), [dead-letter exchanges](https://www.rabbitmq.com/docs/dlx), [Amazon MQ broker defaults](https://docs.aws.amazon.com/amazon-mq/latest/developer-guide/rabbitmq-defaults.html).

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
    "Retry": true,
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
