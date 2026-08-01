using MqCSFramework;
using MqCSFramework.Samples.Contracts;

namespace MqCSFramework.Samples.Consumer;

public class StockProcessor : RpcProcessor<StockRequest, StockResponse>, IStockProcessor
{
    public override Task<StockResponse> ProcessAsync(StockRequest request, MessageContext context, CancellationToken ct = default)
    {
        Console.WriteLine($"[Consumer] Checking stock for SKU {request.Sku}, quantity {request.Quantity}");

        var response = new StockResponse(Available: true, RemainingStock: 42, UnitPrice: 19.99m);
        return Task.FromResult(response);
    }
}
