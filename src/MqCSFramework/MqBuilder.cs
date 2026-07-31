using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MqCSFramework.Internal;

namespace MqCSFramework;

/// <summary>
/// Fluent builder for configuring MqCSFramework senders and consumers.
/// </summary>
public sealed class MqBuilder
{
    private readonly IServiceCollection _services;
    private readonly List<ConsumerRegistration> _consumers = [];

    internal MqBuilder(IServiceCollection services)
    {
        _services = services;
    }

    /// <summary>
    /// Register a standard (fire-and-forget) sender as a keyed IStandardSender singleton.
    /// </summary>
    public MqBuilder AddSender(string name, Action<StandardSenderOptions> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new StandardSenderOptions
        {
            Connection = new RabbitMqConnectionOptions { HostName = "localhost" },
            Exchange = ""
        };
        configure(options);

        _services.AddKeyedSingleton<IStandardSender>(name, (sp, _) =>
        {
            var logger = sp.GetRequiredService<ILogger<RabbitMqStandardSender>>();
            var connection = new RabbitMqConnection(options.Connection, logger);
            return new RabbitMqStandardSender(connection, options, logger);
        });

        return this;
    }

    /// <summary>
    /// Register an RPC (request-reply) sender as a keyed IRpcSender singleton.
    /// </summary>
    public MqBuilder AddRpcSender(string name, Action<RpcSenderOptions> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new RpcSenderOptions
        {
            Connection = new RabbitMqConnectionOptions { HostName = "localhost" },
            Exchange = ""
        };
        configure(options);

        _services.AddKeyedSingleton<IRpcSender>(name, (sp, _) =>
        {
            var logger = sp.GetRequiredService<ILogger<RabbitMqRpcSender>>();
            var connection = new RabbitMqConnection(options.Connection, logger);
            return new RabbitMqRpcSender(connection, options, logger);
        });

        return this;
    }

    /// <summary>
    /// Register a consumer that listens on a queue and dispatches messages to processors.
    /// </summary>
    public MqBuilder AddConsumer(string name, Action<ConsumerOptions> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new ConsumerOptions
        {
            Connection = new RabbitMqConnectionOptions { HostName = "localhost" },
            QueueName = ""
        };
        configure(options);

        _consumers.Add(new ConsumerRegistration(name, options));
        return this;
    }

    /// <summary>
    /// Finalizes the builder. Registers the hosted service if consumers are configured.
    /// </summary>
    internal void Build()
    {
        if (_consumers.Count > 0)
        {
            var registrations = _consumers.ToList().AsReadOnly();

            _services.AddSingleton<IHostedService>(sp =>
            {
                var serviceProvider = sp;
                var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
                var logger = sp.GetRequiredService<ILogger<ConsumerHostedService>>();
                return new ConsumerHostedService(registrations, serviceProvider, loggerFactory, logger);
            });
        }
    }
}
