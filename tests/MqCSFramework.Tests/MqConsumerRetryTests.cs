using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using MqCSFramework.Consumer.Internal;
using MqCSFramework.Internal;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MqCSFramework.Tests;

/// <summary>
/// Failure handling of <see cref="MqConsumer"/> (Requirements 9.7, 9.8), run against a fake channel
/// that records every call, so no broker is needed.
/// </summary>
public class MqConsumerRetryTests
{
    private const string QueueName = "orders-queue";
    private const ulong DeliveryTag = 7;

    [Fact]
    public async Task BrokerRetry_RejectsWithRequeue_AndDoesNotRepublish()
    {
        var options = new ConsumerOptions { QueueName = QueueName, BrokerRetry = true, DeadLetterExchange = "dlx" };
        var channel = FakeChannel.Create();

        await HandleFailureAsync(options, channel, CreateDelivery(retryCount: null));

        var call = Assert.Single(channel.Calls);
        Assert.Equal(nameof(IChannel.BasicRejectAsync), call.Name);
        Assert.Equal(DeliveryTag, call.Args[0]);
        Assert.Equal(true, call.Args[1]); // requeue
    }

    [Fact]
    public async Task FrameworkRetry_RepublishesToOwnQueueViaDefaultExchange_ThenAcks()
    {
        var options = new ConsumerOptions { QueueName = QueueName, MaxRetries = 3 };
        var channel = FakeChannel.Create();

        // Delivered through a fanout exchange with some other routing key: the copy must not go back there.
        await HandleFailureAsync(options, channel, CreateDelivery(retryCount: 1, exchange: "orders-fanout", routingKey: "orders.created"));

        Assert.Equal([nameof(IChannel.BasicPublishAsync), nameof(IChannel.BasicAckAsync)], channel.Calls.Select(c => c.Name));

        var publish = channel.Calls[0];
        Assert.Equal("", publish.Args[0]);
        Assert.Equal(QueueName, publish.Args[1]);
        var props = Assert.IsType<BasicProperties>(publish.Args[3]);
        Assert.Equal(2, props.Headers![MqHeaders.RetryCount]);
        Assert.True(props.Persistent);

        Assert.Equal(DeliveryTag, channel.Calls[1].Args[0]);
    }

    [Fact]
    public async Task MaxRetriesZero_WithoutDeadLetter_NacksWithoutRequeue()
    {
        var options = new ConsumerOptions { QueueName = QueueName, MaxRetries = 0 };
        var channel = FakeChannel.Create();

        await HandleFailureAsync(options, channel, CreateDelivery(retryCount: null));

        var call = Assert.Single(channel.Calls);
        Assert.Equal(nameof(IChannel.BasicNackAsync), call.Name);
        Assert.Equal(DeliveryTag, call.Args[0]);
        Assert.Equal(false, call.Args[2]); // requeue
    }

    [Fact]
    public async Task MaxRetriesZero_WithDeadLetter_PublishesToDeadLetterExchange_ThenAcks()
    {
        var options = new ConsumerOptions
        {
            QueueName = QueueName,
            MaxRetries = 0,
            DeadLetterExchange = "orders-dlx",
            DeadLetterRoutingKey = "orders.dead"
        };
        var channel = FakeChannel.Create();

        await HandleFailureAsync(options, channel, CreateDelivery(retryCount: null));

        Assert.Equal([nameof(IChannel.BasicPublishAsync), nameof(IChannel.BasicAckAsync)], channel.Calls.Select(c => c.Name));
        Assert.Equal("orders-dlx", channel.Calls[0].Args[0]);
        Assert.Equal("orders.dead", channel.Calls[0].Args[1]);
    }

    private static BasicDeliverEventArgs CreateDelivery(int? retryCount, string exchange = "", string routingKey = QueueName)
    {
        var headers = new Dictionary<string, object?>();
        if (retryCount is { } count)
        {
            headers[MqHeaders.RetryCount] = count;
        }

        var props = new BasicProperties { MessageId = "msg-1", Persistent = true, Headers = headers };
        return new BasicDeliverEventArgs("ctag", DeliveryTag, redelivered: false, exchange, routingKey, props, "{}"u8.ToArray());
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
