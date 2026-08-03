using Microsoft.Extensions.Logging;
using MqCSFramework;
using MqCSFramework.Samples.Contracts;

namespace MqCSFramework.Samples.Consumer;

public class OrderProcessor(ILogger<OrderProcessor> logger) : StandardProcessor<OrderMessage>, IOrderProcessor
{
    public override Task ProcessAsync(OrderMessage message, MessageContext context, CancellationToken ct = default)
    {
        logger.LogInformation("---- Order Message Received ----");
        logger.LogInformation("MessageId:     {MessageId}", context.MessageId);
        logger.LogInformation("CorrelationId: {CorrelationId}", context.CorrelationId);
        logger.LogInformation("Timestamp:     {Timestamp}", context.Timestamp);
        logger.LogInformation("Pattern:       {Pattern}", context.Pattern);
        logger.LogInformation("OrderId:       {OrderId}", message.OrderId);
        logger.LogInformation("Customer:      {Customer}", message.CustomerName);
        logger.LogInformation("Amount:        {Amount}", message.Amount);
        logger.LogInformation("CreatedAt:     {CreatedAt}", message.CreatedAt);
        logger.LogInformation("---- Processing Complete ----");
        return Task.CompletedTask;
    }
}
