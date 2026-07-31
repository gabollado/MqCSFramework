using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using MqCSFramework.Abstractions.Consumer;
using MqCSFramework.Abstractions.Internal;
using MqCSFramework.Abstractions.Sender;
using MqCSFramework.Abstractions.Serialization;
using MqCSFramework.Abstractions.Transport;
using MqCSFramework.InMemory;

namespace MqCSFramework.Hosting;

/// <summary>
/// Builder for configuring MqCSFramework services.
/// Processors are registered separately by the user as standard DI singletons.
/// </summary>
public sealed class MqCSFrameworkBuilder
{
    private readonly List<string> _connectionNames = [];
    private bool _serializerRegistered;
    private bool _healthChecksEnabled;

    public IServiceCollection Services { get; }

    internal MqCSFrameworkBuilder(IServiceCollection services)
    {
        Services = services;
    }

    /// <summary>
    /// Register a standard sender as a keyed service (placeholder for RabbitMQ).
    /// </summary>
    public MqCSFrameworkBuilder AddSender(string name, Action<object> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        Services.AddKeyedSingleton<IMessageSender>(name, (_, _) =>
            throw new InvalidOperationException(
                $"Sender '{name}' is registered as a placeholder. " +
                "Add the MqCSFramework.RabbitMQ package and use AddRabbitMqSender() to provide the implementation."));

        _connectionNames.Add(name);
        return this;
    }

    /// <summary>
    /// Register an RPC sender as a keyed service (placeholder for RabbitMQ).
    /// </summary>
    public MqCSFrameworkBuilder AddRpcSender(string name, Action<object> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        Services.AddKeyedSingleton<IRpcSender>(name, (_, _) =>
            throw new InvalidOperationException(
                $"RPC sender '{name}' is registered as a placeholder. " +
                "Add the MqCSFramework.RabbitMQ package and use AddRabbitMqRpcSender() to provide the implementation."));

        _connectionNames.Add(name);
        return this;
    }

    /// <summary>
    /// Register a consumer (placeholder for RabbitMQ).
    /// </summary>
    public MqCSFrameworkBuilder AddConsumer(string name, Action<object> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        _connectionNames.Add(name);
        return this;
    }

    /// <summary>
    /// Register an in-memory sender as a keyed IMessageSender.
    /// </summary>
    public MqCSFrameworkBuilder AddInMemorySender(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        Services.AddKeyedSingleton(name, (_, _) => new InMemoryTransportConnection(name));

        Services.AddKeyedSingleton<ITransportConnection>(name, (sp, key) =>
            sp.GetRequiredKeyedService<InMemoryTransportConnection>((string)key!));

        Services.AddKeyedSingleton<IMessageSender>(name, (sp, key) =>
        {
            var connection = sp.GetRequiredKeyedService<InMemoryTransportConnection>((string)key!);
            var serializer = sp.GetRequiredService<IMessageSerializer>();
            return new InMemoryStandardSender(connection, serializer);
        });

        _connectionNames.Add(name);
        return this;
    }

    /// <summary>
    /// Register an in-memory consumer as an IMessageConsumer.
    /// </summary>
    public MqCSFrameworkBuilder AddInMemoryConsumer(string name, string queueName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);

        Services.AddKeyedSingleton(name, (_, _) => new InMemoryTransportConnection(name));

        Services.AddKeyedSingleton<ITransportConnection>(name, (sp, key) =>
            sp.GetRequiredKeyedService<InMemoryTransportConnection>((string)key!));

        Services.AddSingleton<IMessageConsumer>(sp =>
        {
            var connection = sp.GetRequiredKeyedService<InMemoryTransportConnection>(name);
            var dispatcher = sp.GetRequiredService<MessageDispatcher>();
            var logger = sp.GetRequiredService<ILogger<InMemoryConsumer>>();
            return new InMemoryConsumer(connection, dispatcher, queueName, logger);
        });

        _connectionNames.Add(name);
        return this;
    }

    /// <summary>
    /// Add health checks for all registered sender/consumer connections.
    /// </summary>
    public MqCSFrameworkBuilder AddHealthChecks()
    {
        _healthChecksEnabled = true;
        return this;
    }

    /// <summary>
    /// Replace the default System.Text.Json serializer with a custom implementation.
    /// </summary>
    public MqCSFrameworkBuilder UseSerializer<TSerializer>()
        where TSerializer : class, IMessageSerializer
    {
        Services.AddSingleton<IMessageSerializer, TSerializer>();
        _serializerRegistered = true;
        return this;
    }

    /// <summary>
    /// Finalizes the builder: registers MessageDispatcher, default serializer, and hosted service.
    /// </summary>
    internal void Build()
    {
        if (!_serializerRegistered)
        {
            Services.TryAddSingleton<IMessageSerializer, JsonMessageSerializer>();
        }

        // Register MessageDispatcher — resolves processors directly from DI at runtime
        Services.TryAddSingleton(sp =>
        {
            var serializer = sp.GetRequiredService<IMessageSerializer>();
            var logger = sp.GetRequiredService<ILogger<MessageDispatcher>>();
            return new MessageDispatcher(sp, serializer, logger);
        });

        // Register ConsumerHostedService to manage consumer lifecycle
        Services.TryAddSingleton<Microsoft.Extensions.Hosting.IHostedService>(sp =>
        {
            var consumers = sp.GetServices<IMessageConsumer>();
            var logger = sp.GetRequiredService<ILogger<ConsumerHostedService>>();
            return new ConsumerHostedService(consumers, logger);
        });

        // Register health checks if enabled
        if (_healthChecksEnabled)
        {
            foreach (var connectionName in _connectionNames)
            {
                Services.AddKeyedSingleton<IHealthCheck>(connectionName, (sp, key) =>
                {
                    var connection = sp.GetRequiredKeyedService<ITransportConnection>((string)key!);
                    return new TransportHealthCheck(connection);
                });
            }
        }
    }
}
