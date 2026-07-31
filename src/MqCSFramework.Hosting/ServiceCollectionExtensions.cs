using Microsoft.Extensions.DependencyInjection;

namespace MqCSFramework.Hosting;

/// <summary>
/// Extension methods for registering MqCSFramework services with the DI container.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds MqCSFramework services to the service collection.
    /// Configure senders, consumers, processors, and serialization via the builder action.
    /// </summary>
    public static IServiceCollection AddMqCSFramework(
        this IServiceCollection services,
        Action<MqCSFrameworkBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new MqCSFrameworkBuilder(services);
        configure(builder);
        builder.Build();

        return services;
    }
}
