namespace MqCSFramework;

/// <summary>
/// Configuration for a message consumer.
/// </summary>
public sealed class ConsumerOptions
{
    public required RabbitMqConnectionOptions Connection { get; set; }
    public required string QueueName { get; set; }
    public ushort PrefetchCount { get; set; } = 10;
    public int MaxRetries { get; set; } = 3;
    public string? DeadLetterExchange { get; set; }
    public string? DeadLetterRoutingKey { get; set; }
    public bool SuppressMessageBodyLogging { get; set; }
    public IReadOnlyList<string> MaskedFields { get; set; } = [];
}
