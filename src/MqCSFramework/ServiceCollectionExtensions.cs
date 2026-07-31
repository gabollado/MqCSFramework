using Microsoft.Extensions.DependencyInjection;

namespace MqCSFramework;

/// <summary>
/// Extension methods for registering MqCSFramework services.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds MqCSFramework services to the DI container.
    /// </summary>
    public static IServiceCollection AddMqCSFramework(
        this IServiceCollection services,
        Action<MqBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new MqBuilder(services);
        configure(builder);
        builder.Build();

        return services;
    }
}
