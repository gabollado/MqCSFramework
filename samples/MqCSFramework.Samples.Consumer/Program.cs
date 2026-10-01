using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MqCSFramework.Consumer;
using MqCSFramework.Samples.Contracts;
using Serilog;

namespace MqCSFramework.Samples.Consumer;

public class Program
{
    public static async Task Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);

        // Load local config override (git-ignored, contains connection credentials)
        builder.Configuration.AddJsonFile("appsettings.local.json", optional: true, reloadOnChange: false);

        // Configure Serilog from appsettings.json
        builder.Services.AddSerilog(config => config.ReadFrom.Configuration(builder.Configuration));

        // Register processors as scoped DI services (one instance per message scope)
        builder.Services.AddScoped<IOrderProcessor, OrderProcessor>();
        builder.Services.AddScoped<IStockProcessor, StockProcessor>();

        // Configure MqCSFramework consumers from appsettings.json
        builder.Services.AddMqConsumersFromConfiguration(builder.Configuration);

        Console.WriteLine("[Consumer] Starting...");
        await builder.Build().RunAsync();
    }
}
