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
    /// Timeout for establishing the initial TCP/AMQP connection.
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
