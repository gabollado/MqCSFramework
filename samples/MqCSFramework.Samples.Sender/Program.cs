using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MqCSFramework;
using MqCSFramework.Samples.Contracts;
using Serilog;

var builder = Host.CreateApplicationBuilder(args);

// Configure Serilog from appsettings.json
builder.Services.AddSerilog(config => config.ReadFrom.Configuration(builder.Configuration));

// Configure MqCSFramework — one line, reads everything from "MqCSFramework" section
builder.Services.AddMqCSFramework(builder.Configuration);

var app = builder.Build();

// Standard send
var standardSender = app.Services.GetRequiredKeyedService<IStandardSender>("orders");
var messageId = await standardSender.SendAsync<IOrderProcessor, OrderMessage>(
    new OrderMessage(Guid.NewGuid(), "Alice", 99.99m, DateTimeOffset.UtcNow));

Console.WriteLine($"[Sender] Order sent: {messageId}");

// RPC send
var rpcSender = app.Services.GetRequiredKeyedService<IRpcSender>("stock");
var response = await rpcSender.SendAsync<IStockProcessor, StockResponse, StockRequest>(
    new StockRequest("SKU-12345", 2));

Console.WriteLine($"[Sender] Stock check: Available={response.Available}, Remaining={response.RemainingStock}");
