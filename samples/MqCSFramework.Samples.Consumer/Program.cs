using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MqCSFramework.Hosting;
using MqCSFramework.Samples.Consumer;
using MqCSFramework.Samples.Contracts;

var builder = Host.CreateApplicationBuilder(args);

// Register processors as standard DI singletons
builder.Services.AddSingleton<ISampleProcessor, SampleProcessor>();

// Configure the framework (consumer only — no processor registration needed here)
builder.Services.AddMqCSFramework(mq =>
{
    mq.AddInMemoryConsumer("sample-consumer", "sample-queue");
});

Console.WriteLine("[Consumer] Starting sample consumer on queue 'sample-queue'...");

var app = builder.Build();
await app.RunAsync();
