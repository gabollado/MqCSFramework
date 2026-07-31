using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MqCSFramework.Abstractions.Consumer;

namespace MqCSFramework.Hosting;

/// <summary>
/// BackgroundService that manages consumer lifecycle.
/// Starts all registered consumers in parallel and stops them gracefully on shutdown.
/// </summary>
public sealed class ConsumerHostedService : BackgroundService
{
    private static readonly TimeSpan GracefulStopTimeout = TimeSpan.FromSeconds(30);

    private readonly IEnumerable<IMessageConsumer> _consumers;
    private readonly ILogger<ConsumerHostedService> _logger;

    public ConsumerHostedService(IEnumerable<IMessageConsumer> consumers, ILogger<ConsumerHostedService> logger)
    {
        _consumers = consumers;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumerList = _consumers.ToList();

        if (consumerList.Count == 0)
        {
            _logger.LogWarning("No message consumers registered. ConsumerHostedService will idle");
            await Task.Delay(Timeout.Infinite, stoppingToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            return;
        }

        _logger.LogInformation("Starting {Count} message consumer(s)", consumerList.Count);

        await StartAllConsumersAsync(consumerList, stoppingToken);

        // Wait indefinitely until cancellation is requested
        await Task.Delay(Timeout.Infinite, stoppingToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

        // Graceful shutdown
        _logger.LogInformation("Shutdown requested. Stopping {Count} consumer(s)...", consumerList.Count);
        await StopAllConsumersAsync(consumerList);
    }

    private async Task StartAllConsumersAsync(List<IMessageConsumer> consumers, CancellationToken ct)
    {
        var startTasks = consumers.Select(consumer => StartConsumerAsync(consumer, ct));
        await Task.WhenAll(startTasks);

        _logger.LogInformation("All consumers started successfully");
    }

    private async Task StartConsumerAsync(IMessageConsumer consumer, CancellationToken ct)
    {
        try
        {
            _logger.LogInformation("Starting consumer for queue '{QueueName}'", consumer.QueueName);
            await consumer.StartAsync(ct);
            _logger.LogInformation("Consumer for queue '{QueueName}' started", consumer.QueueName);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Failed to start consumer for queue '{QueueName}'", consumer.QueueName);
            throw;
        }
    }

    private async Task StopAllConsumersAsync(List<IMessageConsumer> consumers)
    {
        using var timeoutCts = new CancellationTokenSource(GracefulStopTimeout);

        var stopTasks = consumers.Select(consumer => StopConsumerAsync(consumer, timeoutCts.Token));

        try
        {
            await Task.WhenAll(stopTasks);
            _logger.LogInformation("All consumers stopped gracefully");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "One or more consumers did not stop cleanly within the timeout");
        }
    }

    private async Task StopConsumerAsync(IMessageConsumer consumer, CancellationToken ct)
    {
        try
        {
            _logger.LogInformation("Stopping consumer for queue '{QueueName}'", consumer.QueueName);
            await consumer.StopAsync(ct);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error stopping consumer for queue '{QueueName}'", consumer.QueueName);
        }
    }
}
