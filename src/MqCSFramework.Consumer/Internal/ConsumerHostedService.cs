using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MqCSFramework.Internal;

namespace MqCSFramework.Consumer.Internal;

/// <summary>
/// BackgroundService that starts and manages all registered consumers.
/// </summary>
internal sealed class ConsumerHostedService : BackgroundService
{
    // Wait between attempts to start a consumer that failed to start (e.g. its broker is unreachable).
    private static readonly TimeSpan StartRetryDelay = TimeSpan.FromSeconds(10);

    private readonly IReadOnlyList<ConsumerRegistration> _registrations;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<ConsumerHostedService> _logger;

    // Added concurrently by the start loops.
    private readonly ConcurrentBag<MqConsumer> _consumers = [];

    public ConsumerHostedService(
        IEnumerable<ConsumerRegistration> registrations,
        IServiceScopeFactory scopeFactory,
        ILoggerFactory loggerFactory,
        ILogger<ConsumerHostedService> logger)
    {
        _registrations = registrations.ToList();
        _scopeFactory = scopeFactory;
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

        // Each consumer starts on its own, so one that can't start doesn't hold up the others.
        await Task.WhenAll(_registrations.Select(reg => StartWithRetryAsync(reg, stoppingToken)));

        if (!stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("All consumers started");
        }

        await Task.Delay(Timeout.Infinite, stoppingToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

        _logger.LogInformation("Shutdown requested. Disposing consumers...");
        foreach (var consumer in _consumers)
        {
            await consumer.DisposeAsync();
        }
    }

    /// <summary>
    /// Starts one consumer, retrying every <see cref="StartRetryDelay"/> until it starts or the host stops.
    /// Never throws: an exception escaping ExecuteAsync would stop the whole host.
    /// </summary>
    private async Task StartWithRetryAsync(ConsumerRegistration registration, CancellationToken stoppingToken)
    {
        ConfigurationChecks.LogUnknownKeys(_logger, registration.UnknownKeys);

        while (!stoppingToken.IsCancellationRequested)
        {
            var consumer = new MqConsumer(registration.Options, _scopeFactory, _loggerFactory.CreateLogger<MqConsumer>());
            try
            {
                await consumer.StartAsync(stoppingToken);
                _consumers.Add(consumer);
                return;
            }
            catch (Exception ex)
            {
                // The failed attempt may hold a half-open connection; the next attempt starts fresh.
                await consumer.DisposeAsync();

                if (stoppingToken.IsCancellationRequested)
                {
                    return;
                }

                _logger.LogError(ex, "Consumer '{Name}' failed to start. Retrying in {Delay}s.",
                    registration.Name, StartRetryDelay.TotalSeconds);
            }

            await Task.Delay(StartRetryDelay, stoppingToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }
    }
}

/// <param name="UnknownKeys">Configuration paths that matched no setting; logged as warnings when the consumer starts.</param>
internal sealed record ConsumerRegistration(string Name, ConsumerOptions Options, IReadOnlyList<string> UnknownKeys);
