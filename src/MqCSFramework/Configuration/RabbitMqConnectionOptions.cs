namespace MqCSFramework;

/// <summary>
/// RabbitMQ connection settings. Each sender/consumer carries its own instance.
/// </summary>
public sealed class RabbitMqConnectionOptions
{
    /// <summary>
    /// One or more comma-separated "host" or "host:port" entries, e.g. "rabbit-1:5671,rabbit-2:5671"
    /// for the nodes of one cluster. An entry without a port uses 5672, or 5671 when <see cref="UseSsl"/>
    /// is true. With several entries the client tries them in random order for the initial connection
    /// and again on automatic recovery.
    /// </summary>
    public string HostName { get; set; } = "localhost";

    public string UserName { get; set; } = "guest";
    public string Password { get; set; } = "guest";
    public string VirtualHost { get; set; } = "/";

    /// <summary>
    /// Enables TLS for every host in <see cref="HostName"/>.
    /// </summary>
    public bool UseSsl { get; set; }

    /// <summary>
    /// Certificate name checked for every host when <see cref="UseSsl"/> is true.
    /// When null or empty, each host is checked against its own name.
    /// </summary>
    public string? SslServerName { get; set; }

    public string? ClientProvidedName { get; set; }

    /// <summary>
    /// Publisher confirms for every publish made through this connection (sender messages, RPC requests,
    /// and the consumer's RPC replies, retry republishes and dead-letter publishes). Default false.
    /// <para>
    /// False: a publish completes once it is written to the socket. A message that no queue matches is
    /// dropped by the broker and a broker rejection goes unnoticed, so <c>SendAsync</c> still succeeds.
    /// </para>
    /// <para>
    /// True: the channel uses publisher confirms and publishes are mandatory, so a publish completes only
    /// after the broker accepts it. No queue matched throws <c>PublishReturnException</c>; a broker
    /// rejection throws <c>PublishException</c>. Each publish then waits for the broker's ack (up to a few
    /// hundred milliseconds for persistent messages on durable or quorum queues under load).
    /// </para>
    /// <para>
    /// Recommended true for standard senders, which get no other feedback. Less important for RPC senders,
    /// where a lost request already ends as <see cref="RpcTimeoutException"/>; with true it fails at once instead.
    /// </para>
    /// </summary>
    public bool ConfirmPublish { get; set; }

    /// <summary>
    /// AMQP heartbeat timeout. The client sends heartbeat frames roughly every
    /// (timeout / 2) seconds, which also keeps otherwise-idle connections alive
    /// against proxies and load balancers that drop idle TCP connections.
    /// The effective value is negotiated with the broker at connection time.
    /// Set to <see cref="TimeSpan.Zero"/> to request disabling heartbeats.
    /// </summary>
    public TimeSpan RequestedHeartbeat { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Enables automatic recovery of the connection (and channels) after a network failure.
    /// </summary>
    public bool AutomaticRecoveryEnabled { get; set; } = true;

    /// <summary>
    /// Enables recovery of topology (queues, exchanges, bindings, consumers) after a reconnect.
    /// Only relevant when <see cref="AutomaticRecoveryEnabled"/> is true.
    /// </summary>
    public bool TopologyRecoveryEnabled { get; set; } = true;

    /// <summary>
    /// Interval between automatic recovery attempts while the connection is down.
    /// </summary>
    public TimeSpan NetworkRecoveryInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Budget per host for opening a connection: TCP connect, TLS and the AMQP handshake. The whole
    /// connect is capped at this value times the number of hosts in <see cref="HostName"/> and then
    /// fails with <see cref="TimeoutException"/>.
    /// </summary>
    public TimeSpan RequestedConnectionTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Timeout for protocol operations (e.g. queue declares, RPC continuations).
    /// </summary>
    public TimeSpan ContinuationTimeout { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>
    /// Number of dispatch "threads" used to deliver messages to consumers on a connection.
    /// When null, the RabbitMQ.Client library default is used.
    /// </summary>
    public ushort? ConsumerDispatchConcurrency { get; set; }
}
