using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MqCSFramework.Internal;

/// <summary>
/// BackgroundService that starts and manages all registered consumers.
/// </summary>
internal sealed class ConsumerHostedService : BackgroundService
{
    private readonly IReadOnlyList<ConsumerRegistration> _registrations;
    private readonly IServiceProvider _serviceProvider;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<ConsumerHostedService> _logger;
    private readonly List<MqConsumer> _consumers = [];

    public ConsumerHostedService(
        IReadOnlyList<ConsumerRegistration> registrations,
        IServiceProvider serviceProvider,
        ILoggerFactory loggerFactory,
        ILogger<ConsumerHostedService> logger)
    {
        _registrations = registrations;
        _serviceProvider = serviceProvider;
        _loggerFactory = loggerFactory;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_registrations.Count == 0)
        {
            _logger.LogWarning("No consumers registered. ConsumerHostedService will idle.");
            await Task.Delay(Timeout.Infinite, stoppingToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            return;
        }

        _logger.LogInformation("Starting {Count} consumer(s)", _registrations.Count);

        foreach (var reg in _registrations)
        {
            var consumer = new MqConsumer(reg.Options, _serviceProvider, _loggerFactory.CreateLogger<MqConsumer>());
            _consumers.Add(consumer);

            await consumer.StartAsync(stoppingToken);
        }

        _logger.LogInformation("All consumers started");

        await Task.Delay(Timeout.Infinite, stoppingToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

        _logger.LogInformation("Shutdown requested. Disposing consumers...");
        foreach (var consumer in _consumers)
        {
            await consumer.DisposeAsync();
        }
    }
}

internal sealed record ConsumerRegistration(string Name, ConsumerOptions Options);
