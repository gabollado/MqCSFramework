using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace MqCSFramework;

/// <summary>
/// Extension methods for registering MqCSFramework services.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds MqCSFramework services with manual builder configuration.
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

    /// <summary>
    /// Adds MqCSFramework services auto-binding from the "MqCSFramework" config section.
    /// </summary>
    public static IServiceCollection AddMqCSFramework(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        return services.AddMqCSFramework(configuration, "MqCSFramework");
    }

    /// <summary>
    /// Adds MqCSFramework services auto-binding from the specified config section name.
    /// </summary>
    public static IServiceCollection AddMqCSFramework(
        this IServiceCollection services,
        IConfiguration configuration,
        string sectionName)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentException.ThrowIfNullOrWhiteSpace(sectionName);

        return services.AddMqCSFramework(mq =>
        {
            mq.BindConfiguration(configuration.GetSection(sectionName));
        });
    }
}
