using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MqCSFramework;
using MqCSFramework.Sender;
using MqCSFramework.Samples.Contracts;
using Serilog;

var builder = Host.CreateApplicationBuilder(args);

builder.Configuration.AddJsonFile("appsettings.local.json", optional: true, reloadOnChange: false);
builder.Services.AddSerilog(config => config.ReadFrom.Configuration(builder.Configuration));
builder.Services.AddMqSendersFromConfiguration(builder.Configuration);

var app = builder.Build();
var logger = app.Services.GetRequiredService<ILogger<Program>>();

logger.LogInformation("=== MqCSFramework Sample Sender ===");

await SendStandardOrderAsync(app.Services, logger);
await SendRpcStockCheckAsync(app.Services, logger);

logger.LogInformation("=== Done ===");

static async Task SendStandardOrderAsync(IServiceProvider services, Microsoft.Extensions.Logging.ILogger logger)
{
    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
    var sender = services.GetRequiredKeyedService<IStandardSender>("orders");
    var correlationId = Guid.NewGuid().ToString("N");

    using (logger.CorrelationScope(correlationId))
    {
        var order = new OrderMessage(Guid.NewGuid(), "Alice", 99.99m, DateTimeOffset.UtcNow);
        logger.LogInformation("Sending order. OrderId: {OrderId}, Customer: {Customer}, Amount: {Amount}",
            order.OrderId, order.CustomerName, order.Amount);

        var messageId = await sender.SendAsync<IOrderProcessor, OrderMessage>(order, correlationId, ct: cts.Token);
        logger.LogInformation("Order sent. MessageId: {MessageId}", messageId);
    }
}

static async Task SendRpcStockCheckAsync(IServiceProvider services, Microsoft.Extensions.Logging.ILogger logger)
{
    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
    var rpcSender = services.GetRequiredKeyedService<IRpcSender>("stock");
    var correlationId = Guid.NewGuid().ToString("N");

    using (logger.CorrelationScope(correlationId))
    {
        var request = new StockRequest("SKU-12345", 2);
        logger.LogInformation("Sending stock check. SKU: {Sku}, Quantity: {Quantity}", request.Sku, request.Quantity);

        var response = await rpcSender.SendAsync<IStockProcessor, StockResponse, StockRequest>(request, correlationId, ct: cts.Token);
        logger.LogInformation("Stock response: Available={Available}, Stock={Stock}, Price={Price}",
            response.Available, response.RemainingStock, response.UnitPrice);
    }
}
