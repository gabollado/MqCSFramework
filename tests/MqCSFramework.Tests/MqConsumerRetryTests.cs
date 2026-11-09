using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using MqCSFramework.Consumer.Internal;
using MqCSFramework.Internal;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MqCSFramework.Tests;

/// <summary>
/// Failure handling of <see cref="MqConsumer"/> (Requirement 9.3), run against a fake channel
/// that records every call, so no broker is needed.
/// </summary>
public class MqConsumerRetryTests
{
    private const string QueueName = "orders-queue";
    private const ulong DeliveryTag = 7;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Failure_RejectsWithRequeueEqualToRetry_AndDoesNotPublishOrAck(bool retry)
    {
        var options = new ConsumerOptions { QueueName = QueueName, Retry = retry };
        var channel = FakeChannel.Create();

        await HandleFailureAsync(options, channel, CreateDelivery());

        var call = Assert.Single(channel.Calls);
        Assert.Equal(nameof(IChannel.BasicRejectAsync), call.Name);
        Assert.Equal(DeliveryTag, call.Args[0]);
        Assert.Equal(retry, call.Args[1]); // requeue
    }

    private static BasicDeliverEventArgs CreateDelivery()
    {
        var props = new BasicProperties { MessageId = "msg-1", Persistent = true };
        return new BasicDeliverEventArgs("ctag", DeliveryTag, redelivered: false, "", QueueName, props, "{}"u8.ToArray());
    }

    /// <summary>
    /// Builds an MqConsumer wired to the fake channel (StartAsync is skipped, it needs a broker)
    /// and calls its private failure handler.
    /// </summary>
    private static Task HandleFailureAsync(ConsumerOptions options, FakeChannel channel, BasicDeliverEventArgs ea)
    {
        var scopeFactory = new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>();
        var consumer = new MqConsumer(options, scopeFactory, NullLogger<MqConsumer>.Instance);

        // RabbitMqConnection only reads ConfirmPublish here; it never connects.
        SetField(consumer, "_connection", new RabbitMqConnection(options.Connection, NullLogger.Instance));
        SetField(consumer, "_channel", channel.Channel);

        var method = typeof(MqConsumer).GetMethod("HandleFailureAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        return (Task)method.Invoke(consumer, [ea, "msg-1"])!;
    }

    private static void SetField(object target, string name, object value) =>
        target.GetType().GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)!.SetValue(target, value);

    /// <summary>
    /// <see cref="IChannel"/> proxy that records each call and completes it immediately.
    /// </summary>
    public class FakeChannel : DispatchProxy
    {
        public List<(string Name, object?[] Args)> Calls { get; } = [];

        public IChannel Channel => (IChannel)(object)this;

        public static FakeChannel Create() => (FakeChannel)(object)DispatchProxy.Create<IChannel, FakeChannel>();

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            var method = targetMethod!;
            if (method.Name == "get_IsOpen")
            {
                return true;
            }

            Calls.Add((method.Name, args ?? []));

            if (method.ReturnType == typeof(ValueTask))
            {
                return ValueTask.CompletedTask;
            }

            if (method.ReturnType == typeof(Task))
            {
                return Task.CompletedTask;
            }

            return method.ReturnType.IsValueType ? Activator.CreateInstance(method.ReturnType) : null;
        }
    }
}
