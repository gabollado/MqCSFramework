using MqCSFramework;
using MqCSFramework.Samples.Contracts;

namespace MqCSFramework.Samples.Consumer;

public class OrderProcessor : StandardProcessor<OrderMessage>, IOrderProcessor
{
    public override Task ProcessAsync(OrderMessage message, MessageContext context, CancellationToken ct = default)
    {
        Console.WriteLine($"[Consumer] Processing order {message.OrderId} for {message.CustomerName} - Amount: {message.Amount:C}");
        return Task.CompletedTask;
    }
}
