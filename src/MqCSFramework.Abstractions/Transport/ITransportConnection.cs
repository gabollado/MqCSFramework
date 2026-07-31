namespace MqCSFramework.Abstractions.Transport;

/// <summary>
/// Represents a connection to a message broker. Manages connection lifecycle.
/// One instance per sender or consumer — NOT shared globally.
/// </summary>
public interface ITransportConnection : IAsyncDisposable
{
    string Name { get; }
    bool IsConnected { get; }
    Task ConnectAsync(CancellationToken ct = default);
    Task<ITransportChannel> CreateChannelAsync(CancellationToken ct = default);
    event Func<Exception, Task>? ConnectionLost;
    event Func<Task>? ConnectionRecovered;
}
