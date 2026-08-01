namespace MqCSFramework;

/// <summary>
/// RabbitMQ connection settings. Each sender/consumer carries its own instance.
/// </summary>
public sealed class RabbitMqConnectionOptions
{
    public string HostName { get; set; } = "localhost";
    public int Port { get; set; } = 5672;
    public string UserName { get; set; } = "guest";
    public string Password { get; set; } = "guest";
    public string VirtualHost { get; set; } = "/";
    public bool UseSsl { get; set; }
    public string? ClientProvidedName { get; set; }
}
