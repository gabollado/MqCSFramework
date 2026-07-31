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
            HostName = _options.HostName,
            Port = _options.Port,
            UserName = _options.UserName,
            Password = _options.Password,
            VirtualHost = _options.VirtualHost,
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true,
            ClientProvidedName = _options.ClientProvidedName
        };

        if (_options.UseSsl)
        {
            factory.Ssl = new SslOption
            {
                Enabled = true,
                ServerName = _options.HostName
            };
        }

        _logger.LogInformation("Connecting to RabbitMQ at {Host}:{Port}/{VHost} as {ClientName}",
            _options.HostName, _options.Port, _options.VirtualHost, _options.ClientProvidedName ?? "unnamed");

        var connection = await factory.CreateConnectionAsync(ct);
        return connection;
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
