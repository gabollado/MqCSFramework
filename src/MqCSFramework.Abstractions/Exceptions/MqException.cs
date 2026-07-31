namespace MqCSFramework.Abstractions.Exceptions;

/// <summary>
/// Base exception for all MqCSFramework errors.
/// </summary>
public class MqException : Exception
{
    public MqException() { }
    public MqException(string message) : base(message) { }
    public MqException(string message, Exception innerException) : base(message, innerException) { }
}
