using System.Collections.Concurrent;
using System.Threading.Channels;
using MqCSFramework.Abstractions.Models;
using MqCSFramework.Abstractions.Transport;

namespace MqCSFramework.InMemory;

/// <summary>
/// In-memory transport connection. Routes messages through Channel&lt;T&gt; queues.
/// Always "connected" — no network calls.
/// Multiple channels can be created from one connection; all share the same queue dictionary.
/// </summary>
public sealed class InMemoryTransportConnection : ITransportConnection
{
    private readonly ConcurrentDictionary<string, Channel<MessageEnvelope>> _queues = new();

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
        ITransportChannel channel = new InMemoryTransportChannel(_queues);
        return Task.FromResult(channel);
    }

    /// <summary>
    /// Gets or creates the underlying Channel&lt;MessageEnvelope&gt; for a given queue name.
    /// Exposed for testing or advanced scenarios where direct queue access is needed.
    /// </summary>
    internal Channel<MessageEnvelope> GetOrCreateQueue(string queueName)
    {
        return _queues.GetOrAdd(queueName, _ => Channel.CreateUnbounded<MessageEnvelope>());
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    // Suppress unused event warnings — events exist to satisfy the interface contract.
    // In-memory connections never disconnect, so these are never raised.
    private void SuppressWarnings()
    {
        _ = ConnectionLost;
        _ = ConnectionRecovered;
    }
}
