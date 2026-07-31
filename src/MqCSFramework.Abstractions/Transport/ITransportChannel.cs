using MqCSFramework.Abstractions.Models;

namespace MqCSFramework.Abstractions.Transport;

/// <summary>
/// A channel over a transport connection. Handles publish/consume operations.
/// </summary>
public interface ITransportChannel : IAsyncDisposable
{
    Task PublishAsync(MessageEnvelope envelope, CancellationToken ct = default);
    Task StartConsumingAsync(string queueName, Func<ReceivedMessage, Task<ProcessResult>> handler, CancellationToken ct = default);
    Task AcknowledgeAsync(ulong deliveryTag, CancellationToken ct = default);
    Task NegativeAcknowledgeAsync(ulong deliveryTag, bool requeue, CancellationToken ct = default);
}
