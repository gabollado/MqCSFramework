using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MqCSFramework;
using MqCSFramework.Samples.Consumer;
using MqCSFramework.Samples.Contracts;

var builder = Host.CreateApplicationBuilder(args);

// Register processors as standard DI singletons
builder.Services.AddSingleton<IOrderProcessor, OrderProcessor>();
builder.Services.AddSingleton<IStockProcessor, StockProcessor>();

builder.Services.AddMqCSFramework(mq =>
{
    mq.AddConsumer("orders", opts =>
    {
        opts.Connection = new RabbitMqConnectionOptions
        {
            HostName = "dog.lmq.cloudamqp.com",
            Port = 5671,
            UserName = "mqxiamut",
            Password = "AcTKNeRmStLhDriJM5mwC3Ok13JgUzOJ",
            VirtualHost = "mqxiamut",
            UseSsl = true,
            ClientProvidedName = "sample-consumer-orders"
        };
        opts.QueueName = "orders-queue";
        opts.PrefetchCount = 20;
        opts.MaxRetries = 3;
    });

    mq.AddConsumer("stock", opts =>
    {
        opts.Connection = new RabbitMqConnectionOptions
        {
            HostName = "dog.lmq.cloudamqp.com",
            Port = 5671,
            UserName = "mqxiamut",
            Password = "AcTKNeRmStLhDriJM5mwC3Ok13JgUzOJ",
            VirtualHost = "mqxiamut",
            UseSsl = true,
            ClientProvidedName = "sample-consumer-stock"
        };
        opts.QueueName = "stock-queue";
        opts.PrefetchCount = 10;
    });
});

Console.WriteLine("[Consumer] Starting...");
await builder.Build().RunAsync();
