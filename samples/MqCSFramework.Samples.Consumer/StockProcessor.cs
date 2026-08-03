using Microsoft.Extensions.Logging;
using MqCSFramework;
using MqCSFramework.Samples.Contracts;

namespace MqCSFramework.Samples.Consumer;

public class StockProcessor(ILogger<StockProcessor> logger) : RpcProcessor<StockRequest, StockResponse>, IStockProcessor
{
    public override Task<StockResponse> ProcessAsync(StockRequest request, MessageContext context, CancellationToken ct = default)
    {
        logger.LogInformation("---- RPC Request Received ----");
        logger.LogInformation("MessageId:     {MessageId}", context.MessageId);
        logger.LogInformation("CorrelationId: {CorrelationId}", context.CorrelationId);
        logger.LogInformation("Timestamp:     {Timestamp}", context.Timestamp);
        logger.LogInformation("Pattern:       {Pattern}", context.Pattern);
        logger.LogInformation("SKU:           {Sku}", request.Sku);
        logger.LogInformation("Quantity:      {Quantity}", request.Quantity);

        var response = new StockResponse(Available: true, RemainingStock: 42, UnitPrice: 19.99m);

        logger.LogInformation("Response:      Available={Available}, Stock={Stock}, Price={Price}",
            response.Available, response.RemainingStock, response.UnitPrice);
        logger.LogInformation("---- Sending Response ----");

        return Task.FromResult(response);
    }
}
