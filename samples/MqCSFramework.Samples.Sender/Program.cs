using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MqCSFramework;
using MqCSFramework.Samples.Contracts;
using Serilog;

var builder = Host.CreateApplicationBuilder(args);

// Load local config override (git-ignored, contains connection credentials)
builder.Configuration.AddJsonFile("appsettings.local.json", optional: true, reloadOnChange: false);

// Configure Serilog from appsettings.json
builder.Services.AddSerilog(config => config.ReadFrom.Configuration(builder.Configuration));

// Configure MqCSFramework — one line, reads everything from "MqCSFramework" section
builder.Services.AddMqCSFramework(builder.Configuration);

var app = builder.Build();
var logger = app.Services.GetRequiredService<ILogger<Program>>();

logger.LogInformation("=== MqCSFramework Sample Sender ===");

// Standard send
logger.LogInformation("Sending standard message (OrderMessage)...");
var standardSender = app.Services.GetRequiredKeyedService<IStandardSender>("orders");

var order = new OrderMessage(Guid.NewGuid(), "Alice", 99.99m, DateTimeOffset.UtcNow);
logger.LogInformation("OrderId: {OrderId}, Customer: {Customer}, Amount: {Amount}",
    order.OrderId, order.CustomerName, order.Amount);

var messageId = await standardSender.SendAsync<IOrderProcessor, OrderMessage>(order);
logger.LogInformation("Standard message sent. MessageId: {MessageId}", messageId);

// RPC send
logger.LogInformation("Sending RPC request (StockRequest)...");
var rpcSender = app.Services.GetRequiredKeyedService<IRpcSender>("stock");

var stockRequest = new StockRequest("SKU-12345", 2);
logger.LogInformation("SKU: {Sku}, Quantity: {Quantity}", stockRequest.Sku, stockRequest.Quantity);

var response = await rpcSender.SendAsync<IStockProcessor, StockResponse, StockRequest>(stockRequest);
logger.LogInformation("RPC Response: Available={Available}, RemainingStock={Stock}, UnitPrice={Price}",
    response.Available, response.RemainingStock, response.UnitPrice);

logger.LogInformation("=== Done ===");
