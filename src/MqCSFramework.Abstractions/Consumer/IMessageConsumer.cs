namespace MqCSFramework.Abstractions.Consumer;

/// <summary>
/// Represents a message consumer that listens on a queue and dispatches to processors.
/// Managed by the hosting layer's BackgroundService.
/// </summary>
public interface IMessageConsumer : IAsyncDisposable
{
    string QueueName { get; }
    Task StartAsync(CancellationToken ct = default);
    Task StopAsync(CancellationToken ct = default);
    bool IsRunning { get; }
}
