using System.Threading.Channels;
using MqCSFramework.Abstractions.Models;
using MqCSFramework.Abstractions.Transport;

namespace MqCSFramework.InMemory;

/// <summary>
/// In-memory transport connection. Routes messages through Channel&lt;T&gt; queues.
/// Always "connected" — no network calls.
/// </summary>
public sealed class InMemoryTransportConnection : ITransportConnection
{
    public string Name { get; }
    public bool IsConnected => true;

    public event Func<Exception, Task>? ConnectionLost;
    public event Func<Task>? ConnectionRecovered;

    public InMemoryTransportConnection(string name)
    {
        Name = name;
    }

    public Task ConnectAsync(CancellationToken ct = default) => Task.CompletedTask;

    public Task<ITransportChannel> CreateChannelAsync(CancellationToken ct = default)
    {
        // TODO: implement in-memory channel
        throw new NotImplementedException();
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
