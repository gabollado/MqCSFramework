using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MqCSFramework;
using MqCSFramework.Samples.Consumer;
using MqCSFramework.Samples.Contracts;
using Serilog;

var builder = Host.CreateApplicationBuilder(args);

// Configure Serilog from appsettings.json
builder.Services.AddSerilog(config => config.ReadFrom.Configuration(builder.Configuration));

// Register processors as standard DI singletons
builder.Services.AddSingleton<IOrderProcessor, OrderProcessor>();
builder.Services.AddSingleton<IStockProcessor, StockProcessor>();

// Configure MqCSFramework — one line, reads everything from "MqCSFramework" section
builder.Services.AddMqCSFramework(builder.Configuration);

Console.WriteLine("[Consumer] Starting...");
await builder.Build().RunAsync();
