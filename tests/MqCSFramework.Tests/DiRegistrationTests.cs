using Microsoft.Extensions.DependencyInjection;
using MqCSFramework;
using MqCSFramework.Sender;
using MqCSFramework.Consumer;

namespace MqCSFramework.Tests;

public class DiRegistrationTests
{
    [Fact]
    public void AddMqSender_RegistersKeyedStandardSender()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMqSender("test", opts =>
        {
            opts.Connection.HostName = "localhost";
            opts.RoutingKey = "test-queue";
        });

        var provider = services.BuildServiceProvider();
        var sender = provider.GetKeyedService<IStandardSender>("test");

        Assert.NotNull(sender);
    }

    [Fact]
    public void AddMqRpcSender_RegistersKeyedRpcSender()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMqRpcSender("test-rpc", opts =>
        {
            opts.Connection.HostName = "localhost";
            opts.RoutingKey = "rpc-queue";
        });

        var provider = services.BuildServiceProvider();
        var sender = provider.GetKeyedService<IRpcSender>("test-rpc");

        Assert.NotNull(sender);
    }

    [Fact]
    public void AddMqConsumer_RegistersConsumerRegistration()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMqConsumer("test-consumer", opts =>
        {
            opts.Connection.HostName = "localhost";
            opts.QueueName = "test-queue";
        });

        var provider = services.BuildServiceProvider();
        var registrations = provider.GetServices<Consumer.Internal.ConsumerRegistration>();

        Assert.Single(registrations);
    }
}
