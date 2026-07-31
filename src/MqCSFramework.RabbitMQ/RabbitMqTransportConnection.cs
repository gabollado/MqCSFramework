using MqCSFramework.Abstractions.Transport;

namespace MqCSFramework.RabbitMQ;

/// <summary>
/// RabbitMQ implementation of ITransportConnection.
/// Manages a single AMQP connection with auto-reconnect.
/// Each sender/consumer gets its own instance.
/// </summary>
public sealed class RabbitMqTransportConnection : ITransportConnection
{
    public string Name { get; }
    public bool IsConnected => false; // TODO: implement

    public event Func<Exception, Task>? ConnectionLost;
    public event Func<Task>? ConnectionRecovered;

    public RabbitMqTransportConnection(string name, RabbitMqConnectionOptions options)
    {
        Name = name;
    }

    public Task ConnectAsync(CancellationToken ct = default)
    {
        // TODO: implement connection logic
        throw new NotImplementedException();
    }

    public Task<ITransportChannel> CreateChannelAsync(CancellationToken ct = default)
    {
        // TODO: implement channel creation
        throw new NotImplementedException();
    }

    public ValueTask DisposeAsync()
    {
        // TODO: implement disposal
        return ValueTask.CompletedTask;
    }
}
