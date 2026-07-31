using Microsoft.Extensions.Logging;

namespace MqCSFramework.RabbitMQ;

/// <summary>
/// Configuration options for a RabbitMQ RPC (request-reply) sender.
/// Each RPC sender carries its own connection options — no shared global connection.
/// </summary>
public sealed class RabbitMqRpcSenderOptions
{
    /// <summary>
    /// Connection properties for this specific RPC sender.
    /// Each RPC sender connects independently.
    /// </summary>
    public RabbitMqConnectionOptions Connection { get; set; } = new();

    public string? Exchange { get; set; }

    public string RoutingKey { get; set; } = "";

    public bool ConfirmSelect { get; set; } = true;

    public IList<string>? MaskedFields { get; set; }

    public LogLevel MessageLogLevel { get; set; } = LogLevel.Information;

    public bool LogMessageBody { get; set; } = true;

    public TimeSpan DefaultTimeout { get; set; } = TimeSpan.FromSeconds(30);

    public int MaxRetryAttempts { get; set; } = 3;
}
