using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MqCSFramework.Abstractions.Consumer;

namespace MqCSFramework.Hosting;

/// <summary>
/// BackgroundService that manages consumer lifecycle.
/// Starts all registered consumers and stops them gracefully on shutdown.
/// </summary>
public sealed class ConsumerHostedService : BackgroundService
{
    private readonly IEnumerable<IMessageConsumer> _consumers;
    private readonly ILogger<ConsumerHostedService> _logger;

    public ConsumerHostedService(IEnumerable<IMessageConsumer> consumers, ILogger<ConsumerHostedService> logger)
    {
        _consumers = consumers;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting {Count} message consumers", _consumers.Count());

        var startTasks = _consumers.Select(c => c.StartAsync(stoppingToken));
        await Task.WhenAll(startTasks);

        _logger.LogInformation("All consumers started successfully");

        // Wait until cancellation is requested
        await Task.Delay(Timeout.Infinite, stoppingToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

        _logger.LogInformation("Stopping consumers...");
        var stopTasks = _consumers.Select(c => c.StopAsync(CancellationToken.None));
        await Task.WhenAll(stopTasks);
    }
}
