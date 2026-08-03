using Microsoft.Extensions.Logging;

namespace MqCSFramework;

/// <summary>
/// Extension method to simplify creating a logging scope with a CorrelationId.
/// </summary>
public static class LoggingExtensions
{
    /// <summary>
    /// Creates a logging scope that includes the CorrelationId in all log entries within the scope.
    /// Requires Serilog's Enrich.FromLogContext and {CorrelationId} in the output template.
    /// </summary>
    public static IDisposable? CorrelationScope(this ILogger logger, string correlationId)
    {
        return logger.BeginScope(new Dictionary<string, object> { ["CorrelationId"] = correlationId });
    }
}
