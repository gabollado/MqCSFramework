namespace MqCSFramework;

/// <summary>
/// Configuration for a message consumer.
/// </summary>
public sealed class ConsumerOptions
{
    public RabbitMqConnectionOptions Connection { get; set; } = new();
    public string QueueName { get; set; } = "";
    public ushort PrefetchCount { get; set; } = 10;
    public int MaxRetries { get; set; } = 3;
    public int ProcessingTimeoutMs { get; set; } = 30000;
    public string? DeadLetterExchange { get; set; }
    public string? DeadLetterRoutingKey { get; set; }
    public IReadOnlyList<string> MaskedFields { get; set; } = [];

    /// <summary>
    /// When true, the queue is declared as a quorum queue and a failed message is rejected with requeue,
    /// so the broker handles retries. Delay, retry limit and dead-lettering come from a RabbitMQ policy
    /// (<c>delayed-retry-type/min/max</c>, <c>delivery-limit</c>, <c>dead-letter-exchange</c>; RabbitMQ 4.3+).
    /// <see cref="MaxRetries"/>, <see cref="DeadLetterExchange"/> and <see cref="DeadLetterRoutingKey"/> are not used.
    /// Default false (framework retry).
    /// </summary>
    public bool BrokerRetry { get; set; }
}
