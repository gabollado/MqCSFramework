using Microsoft.Extensions.Logging;

namespace MqCSFramework.RabbitMQ;

/// <summary>
/// Configuration options for a RabbitMQ standard (fire-and-forget) sender.
/// Each sender carries its own connection options — no shared global connection.
/// </summary>
public sealed class RabbitMqSenderOptions
{
    /// <summary>
    /// Connection properties for this specific sender.
    /// Each sender connects independently.
    /// </summary>
    public RabbitMqConnectionOptions Connection { get; set; } = new();

    public string? Exchange { get; set; }

    public string RoutingKey { get; set; } = "";

    public bool ConfirmSelect { get; set; } = true;

    public IList<string>? MaskedFields { get; set; }

    public LogLevel MessageLogLevel { get; set; } = LogLevel.Information;

    public bool LogMessageBody { get; set; } = true;
}
