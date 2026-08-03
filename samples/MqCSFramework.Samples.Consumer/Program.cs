using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MqCSFramework;
using MqCSFramework.Consumer;
using MqCSFramework.Samples.Consumer;
using MqCSFramework.Samples.Contracts;
using Serilog;

var builder = Host.CreateApplicationBuilder(args);

// Load local config override (git-ignored, contains connection credentials)
builder.Configuration.AddJsonFile("appsettings.local.json", optional: true, reloadOnChange: false);

// Configure Serilog from appsettings.json
builder.Services.AddSerilog(config => config.ReadFrom.Configuration(builder.Configuration));

// Register processors as standard DI singletons
builder.Services.AddSingleton<IOrderProcessor, OrderProcessor>();
builder.Services.AddSingleton<IStockProcessor, StockProcessor>();

// Configure MqCSFramework consumers from appsettings.json
builder.Services.AddMqConsumersFromConfiguration(builder.Configuration);

Console.WriteLine("[Consumer] Starting...");
await builder.Build().RunAsync();
