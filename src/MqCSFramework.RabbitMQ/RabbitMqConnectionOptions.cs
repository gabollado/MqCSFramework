namespace MqCSFramework.RabbitMQ;

/// <summary>
/// Connection options for a RabbitMQ transport.
/// Embedded in each sender/consumer configuration — no shared global connection.
/// </summary>
public sealed class RabbitMqConnectionOptions
{
    public string HostNames { get; set; } = "localhost";
    public string UserName { get; set; } = "guest";
    public string Password { get; set; } = "guest";
    public string VirtualHost { get; set; } = "/";
    public string? SslServerName { get; set; }
    public bool AutomaticRecoveryEnabled { get; set; } = true;
    public TimeSpan NetworkRecoveryInterval { get; set; } = TimeSpan.FromSeconds(5);
}
