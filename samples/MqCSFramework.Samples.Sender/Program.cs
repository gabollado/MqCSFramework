using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MqCSFramework;
using MqCSFramework.Samples.Contracts;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddMqCSFramework(mq =>
{
    mq.AddSender("orders", opts =>
    {
        opts.Connection = new RabbitMqConnectionOptions
        {
            HostName = "dog.lmq.cloudamqp.com",
            Port = 5671,
            UserName = "mqxiamut",
            Password = "AcTKNeRmStLhDriJM5mwC3Ok13JgUzOJ",
            VirtualHost = "mqxiamut",
            UseSsl = true,
            ClientProvidedName = "sample-sender-orders"
        };
        opts.Exchange = "";
        opts.RoutingKey = "orders-queue";
    });

    mq.AddRpcSender("stock", opts =>
    {
        opts.Connection = new RabbitMqConnectionOptions
        {
            HostName = "dog.lmq.cloudamqp.com",
            Port = 5671,
            UserName = "mqxiamut",
            Password = "AcTKNeRmStLhDriJM5mwC3Ok13JgUzOJ",
            VirtualHost = "mqxiamut",
            UseSsl = true,
            ClientProvidedName = "sample-sender-stock"
        };
        opts.Exchange = "";
        opts.RoutingKey = "stock-queue";
        opts.Timeout = TimeSpan.FromSeconds(10);
    });
});

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
