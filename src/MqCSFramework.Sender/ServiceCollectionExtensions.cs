using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MqCSFramework.Internal;
using MqCSFramework.Sender.Internal;

namespace MqCSFramework.Sender;

/// <summary>
/// Extension methods for registering MqCSFramework sender services.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers a standard (fire-and-forget) sender as a keyed IStandardSender singleton.
    /// </summary>
    public static IServiceCollection AddMqSender(
        this IServiceCollection services,
        string name,
        Action<StandardSenderOptions> configure)
    {
        return AddStandardSender(services, name, configure, unknownKeys: []);
    }

    /// <summary>
    /// Registers an RPC (request-reply) sender as a keyed IRpcSender singleton.
    /// </summary>
    public static IServiceCollection AddMqRpcSender(
        this IServiceCollection services,
        string name,
        Action<RpcSenderOptions> configure)
    {
        return AddRpcSender(services, name, configure, unknownKeys: []);
    }

    /// <summary>
    /// Auto-registers all senders and RPC senders from the given config section.
    /// Reads "Senders" and "RpcSenders" sub-sections. Keys that match no setting are logged as warnings
    /// when each sender is created.
    /// </summary>
    public static IServiceCollection AddMqSendersFromConfiguration(
        this IServiceCollection services,
        IConfiguration configuration,
        string sectionName = "MqCSFramework")
    {
        var section = configuration.GetSection(sectionName);

        foreach (var child in section.GetSection("Senders").GetChildren())
        {
            AddStandardSender(services, child.Key, opts => child.Bind(opts),
                ConfigurationChecks.FindUnknownKeys(child, typeof(StandardSenderOptions)));
        }

        foreach (var child in section.GetSection("RpcSenders").GetChildren())
        {
            AddRpcSender(services, child.Key, opts => child.Bind(opts),
                ConfigurationChecks.FindUnknownKeys(child, typeof(RpcSenderOptions)));
        }

        return services;
    }

    private static IServiceCollection AddStandardSender(
        IServiceCollection services,
        string name,
        Action<StandardSenderOptions> configure,
        IReadOnlyList<string> unknownKeys)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new StandardSenderOptions();
        configure(options);

        services.AddKeyedSingleton<IStandardSender>(name, (sp, _) =>
        {
            var logger = sp.GetRequiredService<ILogger<RabbitMqStandardSender>>();
            ConfigurationChecks.LogUnknownKeys(logger, unknownKeys);
            var connection = new RabbitMqConnection(options.Connection, logger);
            return new RabbitMqStandardSender(connection, options, logger);
        });

        return services;
    }

    private static IServiceCollection AddRpcSender(
        IServiceCollection services,
        string name,
        Action<RpcSenderOptions> configure,
        IReadOnlyList<string> unknownKeys)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new RpcSenderOptions();
        configure(options);

        services.AddKeyedSingleton<IRpcSender>(name, (sp, _) =>
        {
            var logger = sp.GetRequiredService<ILogger<RabbitMqRpcSender>>();
            ConfigurationChecks.LogUnknownKeys(logger, unknownKeys);
            var connection = new RabbitMqConnection(options.Connection, logger);
            return new RabbitMqRpcSender(connection, options, logger);
        });

        return services;
    }
}
