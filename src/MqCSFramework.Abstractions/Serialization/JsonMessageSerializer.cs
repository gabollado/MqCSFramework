using System.Text.Json;
using MqCSFramework.Abstractions.Exceptions;

namespace MqCSFramework.Abstractions.Serialization;

/// <summary>
/// Default message serializer using System.Text.Json with sensible defaults.
/// </summary>
public sealed class JsonMessageSerializer : IMessageSerializer
{
    private static readonly JsonSerializerOptions DefaultOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        NumberHandling = System.Text.Json.Serialization.JsonNumberHandling.AllowReadingFromString,
        WriteIndented = false
    };

    private readonly JsonSerializerOptions _options;

    public JsonMessageSerializer()
        : this(null)
    {
    }

    public JsonMessageSerializer(JsonSerializerOptions? options)
    {
        _options = options ?? DefaultOptions;
    }

    public string ContentType => "application/json";

    public byte[] Serialize<T>(T message) where T : class
    {
        try
        {
            return JsonSerializer.SerializeToUtf8Bytes(message, _options);
        }
        catch (JsonException ex)
        {
            throw new MessageSerializationException(
                $"Failed to serialize message of type '{typeof(T).FullName}'.",
                messageId: null,
                targetType: typeof(T),
                innerException: ex);
        }
    }

    public T Deserialize<T>(ReadOnlySpan<byte> data) where T : class
    {
        try
        {
            var result = JsonSerializer.Deserialize<T>(data, _options);
            if (result is null)
            {
                throw new MessageSerializationException(
                    $"Deserialization returned null for type '{typeof(T).FullName}'.",
                    messageId: null,
                    targetType: typeof(T));
            }

            return result;
        }
        catch (MessageSerializationException)
        {
            throw;
        }
        catch (JsonException ex)
        {
            throw new MessageSerializationException(
                $"Failed to deserialize message to type '{typeof(T).FullName}'.",
                messageId: null,
                targetType: typeof(T),
                innerException: ex);
        }
    }

    public object Deserialize(ReadOnlySpan<byte> data, Type type)
    {
        try
        {
            var result = JsonSerializer.Deserialize(data, type, _options);
            if (result is null)
            {
                throw new MessageSerializationException(
                    $"Deserialization returned null for type '{type.FullName}'.",
                    messageId: null,
                    targetType: type);
            }

            return result;
        }
        catch (MessageSerializationException)
        {
            throw;
        }
        catch (JsonException ex)
        {
            throw new MessageSerializationException(
                $"Failed to deserialize message to type '{type.FullName}'.",
                messageId: null,
                targetType: type,
                innerException: ex);
        }
    }
}
