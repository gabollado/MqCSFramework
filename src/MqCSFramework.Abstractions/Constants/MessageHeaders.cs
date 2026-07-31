namespace MqCSFramework.Abstractions.Constants;

/// <summary>
/// Well-known message header names used by the framework.
/// </summary>
public static class MessageHeaders
{
    public const string MessageType = "mq-message-type";
    public const string ProcessorType = "mq-processor-type";
    public const string CorrelationId = "mq-correlation-id";
    public const string SenderIdentity = "mq-sender-identity";
    public const string LocalDateTime = "mq-local-datetime";
    public const string TraceParent = "traceparent";
    public const string TraceState = "tracestate";
}
