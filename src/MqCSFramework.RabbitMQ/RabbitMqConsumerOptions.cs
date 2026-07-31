using Microsoft.Extensions.Logging;

namespace MqCSFramework.RabbitMQ;

/// <summary>
/// Configuration options for a RabbitMQ consumer.
/// Each consumer carries its own connection options — no shared global connection.
/// </summary>
public sealed class RabbitMqConsumerOptions
{
    /// <summary>
    /// Connection properties for this specific consumer.
    /// Each consumer connects independently.
    /// </summary>
    public RabbitMqConnectionOptions Connection { get; set; } = new();

    public string QueueName { get; set; } = "";

    public ushort PrefetchCount { get; set; } = 10;

    public bool AutoAck { get; set; }

    public bool IsRpc { get; set; }

    public int ProcessingTimeoutMs { get; set; } = 30000;

    public int DelayRetryLimit { get; set; }

    public string? ErrorQueueName { get; set; }

    public IList<string>? MaskedFields { get; set; }

    public LogLevel MessageLogLevel { get; set; } = LogLevel.Information;

    public bool LogMessageBody { get; set; } = true;
}
