using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using MqCSFramework;
using MqCSFramework.Samples.Contracts;

namespace MqCSFramework.Samples.Consumer;

public class StockProcessor(ILogger<StockProcessor> logger) : RpcProcessor<StockRequest, StockResponse>, IStockProcessor
{
    private static readonly ConcurrentDictionary<string, int> _stock = new();

    public override Task<StockResponse> ProcessAsync(StockRequest request, MessageContext context, CancellationToken ct = default)
    {
        logger.LogInformation("---- RPC Request Received ----");
        logger.LogInformation("SKU: {Sku}, Quantity requested: {Quantity}", request.Sku, request.Quantity);

        var currentStock = _stock.GetOrAdd(request.Sku, _ => 50);
        var newStock = currentStock - request.Quantity;
        _stock[request.Sku] = newStock;

        var available = newStock >= 0;

        logger.LogInformation("SKU: {Sku}, Previous stock: {Previous}, New stock: {New}, Available: {Available}",
            request.Sku, currentStock, newStock, available);

        var response = new StockResponse(Available: available, RemainingStock: newStock, UnitPrice: 19.99m);
        return Task.FromResult(response);
    }
}
