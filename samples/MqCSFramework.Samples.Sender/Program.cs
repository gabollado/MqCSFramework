using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MqCSFramework.Abstractions.Sender;
using MqCSFramework.Hosting;
using MqCSFramework.Samples.Contracts;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddMqCSFramework(mq =>
{
    mq.AddInMemorySender("sample-sender");
});

var app = builder.Build();

// Resolve the sender and send an RPC request
var sender = app.Services.GetRequiredKeyedService<IMessageSender>("sample-sender");

Console.WriteLine("[Sender] Sending sample request...");

var messageId = await sender.SendAsync<ISampleProcessor>(
    new SampleRequest("World", 42),
    new MqCSFramework.Abstractions.Configuration.SendOptions { RoutingKey = "sample-queue" });

Console.WriteLine($"[Sender] Message sent with ID: {messageId}");
Console.WriteLine("[Sender] Done. (Note: for RPC with response, use IRpcSender)");
