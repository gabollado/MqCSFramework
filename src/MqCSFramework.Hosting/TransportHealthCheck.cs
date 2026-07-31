using Microsoft.Extensions.Diagnostics.HealthChecks;
using MqCSFramework.Abstractions.Transport;

namespace MqCSFramework.Hosting;

/// <summary>
/// Health check for a specific sender/consumer transport connection.
/// One instance per registered sender/consumer.
/// </summary>
public sealed class TransportHealthCheck : IHealthCheck
{
    private readonly ITransportConnection _connection;

    public TransportHealthCheck(ITransportConnection connection)
    {
        _connection = connection;
    }

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (_connection.IsConnected)
        {
            return Task.FromResult(HealthCheckResult.Healthy($"Connection '{_connection.Name}' is active"));
        }

        return Task.FromResult(HealthCheckResult.Degraded($"Connection '{_connection.Name}' is not connected"));
    }
}
