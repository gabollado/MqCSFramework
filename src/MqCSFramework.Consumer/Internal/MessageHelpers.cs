using System.Text;
using RabbitMQ.Client.Events;

namespace MqCSFramework.Consumer.Internal;

/// <summary>
/// Static helper methods for RabbitMQ message header parsing and context building.
/// </summary>
internal static class MessageHelpers
{
    /// <summary>
    /// Reads a header value from BasicDeliverEventArgs as a string.
    /// RabbitMQ stores string headers as byte[] (UTF-8).
    /// </summary>
    public static string? GetHeaderString(BasicDeliverEventArgs ea, string headerName)
    {
        if (ea.BasicProperties?.Headers is null)
        {
            return null;
        }

        if (!ea.BasicProperties.Headers.TryGetValue(headerName, out var value))
        {
            return null;
        }

        return value switch
        {
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            string s => s,
            _ => value?.ToString()
        };
    }

    /// <summary>
    /// Reads the mq-retry-count header as an integer. Returns 0 if not present.
    /// </summary>
    public static int GetRetryCount(BasicDeliverEventArgs ea)
    {
        if (ea.BasicProperties?.Headers is null)
        {
            return 0;
        }

        if (!ea.BasicProperties.Headers.TryGetValue(MqHeaders.RetryCount, out var value))
        {
            return 0;
        }

        return value switch
        {
            int i => i,
            long l => (int)l,
            byte[] bytes => int.TryParse(Encoding.UTF8.GetString(bytes), out var parsed) ? parsed : 0,
            _ => 0
        };
    }

    /// <summary>
    /// Builds a MessageContext from the RabbitMQ delivery event args.
    /// </summary>
    public static MessageContext BuildContext(BasicDeliverEventArgs ea, string messageId, string correlationId, string pattern)
    {
        var headers = new Dictionary<string, string>();
        if (ea.BasicProperties?.Headers is not null)
        {
            foreach (var kvp in ea.BasicProperties.Headers)
            {
                var val = kvp.Value switch
                {
                    byte[] bytes => Encoding.UTF8.GetString(bytes),
                    _ => kvp.Value?.ToString() ?? ""
                };
                headers[kvp.Key] = val;
            }
        }

        var timestamp = ea.BasicProperties?.Timestamp.UnixTime > 0
            ? DateTimeOffset.FromUnixTimeSeconds(ea.BasicProperties.Timestamp.UnixTime)
            : DateTimeOffset.UtcNow;

        return new MessageContext
        {
            MessageId = messageId,
            CorrelationId = correlationId,
            Timestamp = timestamp,
            Pattern = pattern,
            Headers = headers
        };
    }
}
