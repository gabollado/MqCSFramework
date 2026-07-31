using Microsoft.Extensions.Diagnostics.HealthChecks;
using MqCSFramework.Abstractions.Transport;

namespace MqCSFramework.Hosting;

/// <summary>
/// Health check for a specific sender/consumer transport connection.
/// One instance per registered sender/consumer.
/// Reports Healthy/Degraded/Unhealthy based on connection state.
/// </summary>
public sealed class TransportHealthCheck : IHealthCheck, IDisposable
{
    private readonly ITransportConnection _connection;
    private volatile bool _recovering;
    private bool _disposed;

    public TransportHealthCheck(ITransportConnection connection)
    {
        _connection = connection;

        _connection.ConnectionLost += OnConnectionLost;
        _connection.ConnectionRecovered += OnConnectionRecovered;
    }

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (_connection.IsConnected)
        {
            return Task.FromResult(HealthCheckResult.Healthy($"Connection '{_connection.Name}' is active."));
        }

        if (_recovering)
        {
            return Task.FromResult(HealthCheckResult.Degraded($"Connection '{_connection.Name}' is recovering."));
        }

        return Task.FromResult(HealthCheckResult.Unhealthy($"Connection '{_connection.Name}' is disconnected."));
    }

    private Task OnConnectionLost(Exception _)
    {
        _recovering = true;
        return Task.CompletedTask;
    }

    private Task OnConnectionRecovered()
    {
        _recovering = false;
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _connection.ConnectionLost -= OnConnectionLost;
        _connection.ConnectionRecovered -= OnConnectionRecovered;
        _disposed = true;
    }
}
