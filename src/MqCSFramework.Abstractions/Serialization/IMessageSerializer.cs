namespace MqCSFramework.Abstractions.Serialization;

/// <summary>
/// Pluggable message serialization. Default implementation uses System.Text.Json.
/// </summary>
public interface IMessageSerializer
{
    byte[] Serialize<T>(T message) where T : class;
    T Deserialize<T>(ReadOnlySpan<byte> data) where T : class;
    object Deserialize(ReadOnlySpan<byte> data, Type type);
    string ContentType { get; }
}
