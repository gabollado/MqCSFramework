using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MqCSFramework.Consumer.Internal;

namespace MqCSFramework.Consumer;

/// <summary>
/// Extension methods for registering MqCSFramework consumer services.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers a consumer that listens on a queue and dispatches messages to processors.
    /// </summary>
    public static IServiceCollection AddMqConsumer(
        this IServiceCollection services,
        string name,
        Action<ConsumerOptions> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new ConsumerOptions();
        configure(options);

        // Store consumer registrations for the hosted service
        services.AddSingleton(new ConsumerRegistration(name, options));

        // Ensure hosted service is registered (idempotent)
        services.AddHostedService<ConsumerHostedService>();

        return services;
    }

    /// <summary>
    /// Auto-registers all consumers from the given config section.
    /// Reads the "Consumers" sub-section.
    /// </summary>
    public static IServiceCollection AddMqConsumersFromConfiguration(
        this IServiceCollection services,
        IConfiguration configuration,
        string sectionName = "MqCSFramework")
    {
        var section = configuration.GetSection(sectionName);

        foreach (var child in section.GetSection("Consumers").GetChildren())
        {
            services.AddMqConsumer(child.Key, opts => child.Bind(opts));
        }

        return services;
    }
}
