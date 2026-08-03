namespace MqCSFramework;

/// <summary>
/// Well-known header names used by the framework.
/// </summary>
public static class MqHeaders
{
    public const string ProcessorType = "mq-processor-type";
    public const string Pattern = "mq-pattern";
    public const string RetryCount = "mq-retry-count";

    public const string PatternStandard = "standard";
    public const string PatternRpc = "rpc";
    public const string CancellationDeadline = "mq-cancellation-deadline";
}
