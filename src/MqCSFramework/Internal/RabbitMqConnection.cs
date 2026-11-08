using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace MqCSFramework.Internal;

/// <summary>
/// Manages a single RabbitMQ connection and channel for a sender or consumer.
/// Uses lazy initialization and relies on RabbitMQ.Client 7.x built-in automatic recovery.
/// </summary>
internal sealed class RabbitMqConnection : IAsyncDisposable
{
    private readonly RabbitMqConnectionOptions _options;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _lock = new(1, 1);

    private IConnection? _connection;
    private IChannel? _channel;

    public RabbitMqConnection(RabbitMqConnectionOptions options, ILogger logger)
    {
        _options = options;
        _logger = logger;
    }

    public async Task<IChannel> GetChannelAsync(CancellationToken ct = default)
    {
        if (_channel is { IsOpen: true })
        {
            return _channel;
        }

        await _lock.WaitAsync(ct);
        try
        {
            if (_channel is { IsOpen: true })
            {
                return _channel;
            }

            // Close stale channel if exists
            if (_channel is not null)
            {
                try { await _channel.CloseAsync(ct); } catch { /* ignore */ }
                _channel.Dispose();
                _channel = null;
            }

            // Ensure connection is alive
            if (_connection is null or { IsOpen: false })
            {
                if (_connection is not null)
                {
                    try { await _connection.CloseAsync(ct); } catch { /* ignore */ }
                    _connection.Dispose();
                }

                _connection = await CreateConnectionAsync(ct);
            }

            _channel = await _connection.CreateChannelAsync(cancellationToken: ct);
            return _channel;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Resets the channel after a publish failure, forcing a new one on next use.
    /// </summary>
    public async Task ResetChannelAsync()
    {
        await _lock.WaitAsync();
        try
        {
            if (_channel is not null)
            {
                try { await _channel.CloseAsync(); } catch { /* ignore */ }
                _channel.Dispose();
                _channel = null;
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    private async Task<IConnection> CreateConnectionAsync(CancellationToken ct)
    {
        var factory = new ConnectionFactory
        {
            UserName = _options.UserName,
            Password = _options.Password,
            VirtualHost = _options.VirtualHost,
            RequestedHeartbeat = _options.RequestedHeartbeat,
            AutomaticRecoveryEnabled = _options.AutomaticRecoveryEnabled,
            TopologyRecoveryEnabled = _options.TopologyRecoveryEnabled,
            NetworkRecoveryInterval = _options.NetworkRecoveryInterval,
            RequestedConnectionTimeout = _options.RequestedConnectionTimeout,
            ContinuationTimeout = _options.ContinuationTimeout,
            ClientProvidedName = _options.ClientProvidedName
        };

        if (_options.ConsumerDispatchConcurrency is { } concurrency)
        {
            factory.ConsumerDispatchConcurrency = concurrency;
        }

        // The client's own parser: comma-separated "host" or "host:port" entries, IPv6 in brackets.
        // An entry without a port resolves to 5672, or 5671 once TLS is enabled on it.
        var endpoints = AmqpTcpEndpoint.ParseMultiple(_options.HostName);

        // Each endpoint gets its own TLS settings, so without SslServerName a node presenting its own
        // certificate is checked against its own name. The client picks endpoints in random order
        // for the initial connection and for automatic recovery.
        foreach (var endpoint in endpoints)
        {
            endpoint.Ssl = new SslOption
            {
                Enabled = _options.UseSsl,
                ServerName = string.IsNullOrEmpty(_options.SslServerName) ? endpoint.HostName : _options.SslServerName
            };
        }

        var endpointList = string.Join(", ", endpoints.Select(e => $"{e.HostName}:{e.Port}"));
        _logger.LogInformation("Connecting to RabbitMQ at {Endpoints}, vhost {VHost}, as {ClientName}",
            endpointList, _options.VirtualHost, _options.ClientProvidedName ?? "unnamed");

        // RabbitMQ.Client applies RequestedConnectionTimeout to the TCP connect only: when a server accepts
        // TCP but never answers the AMQP handshake (e.g. plaintext to a TLS port), CreateConnectionAsync can
        // wait forever. Cap the whole connect per endpoint, since endpoints are tried one after another.
        var timeout = _options.RequestedConnectionTimeout * endpoints.Length;
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(timeout);

        try
        {
            return await factory.CreateConnectionAsync(endpoints, timeoutCts.Token);
        }
        catch (OperationCanceledException ex) when (timeoutCts.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            throw new TimeoutException($"Timed out after {timeout.TotalSeconds:0}s connecting to RabbitMQ at {endpointList}.", ex);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel is not null)
        {
            try { await _channel.CloseAsync(); } catch { /* ignore */ }
            _channel.Dispose();
            _channel = null;
        }

        if (_connection is not null)
        {
            try { await _connection.CloseAsync(); } catch { /* ignore */ }
            _connection.Dispose();
            _connection = null;
        }

        _lock.Dispose();
    }
}
