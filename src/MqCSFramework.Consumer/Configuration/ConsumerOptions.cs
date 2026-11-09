namespace MqCSFramework;

/// <summary>
/// Configuration for a message consumer. An existing queue is used as it is; a missing queue is
/// declared as a quorum queue.
/// </summary>
public sealed class ConsumerOptions
{
    public RabbitMqConnectionOptions Connection { get; set; } = new();
    public string QueueName { get; set; } = "";
    public ushort PrefetchCount { get; set; } = 10;
    public int ProcessingTimeoutMs { get; set; } = 30000;
    public IReadOnlyList<string> MaskedFields { get; set; } = [];

    /// <summary>
    /// What happens when a processor throws. Default false.
    /// <para>
    /// False: the message is rejected without requeue, so the broker dead-letters it through the queue's
    /// dead-letter exchange, or drops it when there is none. Safe on every queue type.
    /// </para>
    /// <para>
    /// True: the message is rejected with requeue and the broker retries it. On a quorum queue
    /// (RabbitMQ 4.3+) a policy sets the delay (<c>delayed-retry-type/min/max</c>), the limit
    /// (<c>delivery-limit</c>, default 20) and the dead-lettering (<c>dead-letter-exchange</c>).
    /// On a classic queue the message is redelivered immediately and forever: don't use it there.
    /// </para>
    /// </summary>
    public bool Retry { get; set; }
}
