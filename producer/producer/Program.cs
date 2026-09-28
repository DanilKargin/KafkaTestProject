using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using producer.Configuration;
using producer.Infrastructure;
using producer.Services;
using producer.Workers;
using System.Text.Json;

var builder = Host.CreateApplicationBuilder(args);

// Конфиг: appsettings + переменные окружения (KAFKA_BOOTSTRAP, PG_CONN)
builder.Configuration
    .AddJsonFile("appsettings.json", optional: true)
    .AddEnvironmentVariables();

var config = builder.Configuration.Get<AppConfig>() ?? new AppConfig();

// ENV-override для Docker
if (Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP") is { } kb)
    config.Kafka.BootstrapServers = kb;
if (Environment.GetEnvironmentVariable("PG_CONN") is { } pg)
    config.Database.ConnectionString = pg;

builder.Services.AddSingleton(config);
builder.Services.AddSingleton(new Database(config.Database.ConnectionString));
builder.Services.AddSingleton<IProducer<string, string>>(_ =>
    KafkaProducerFactory.Create(config.Kafka));
builder.Services.AddSingleton<IOrderService, OrderService>();
builder.Services.AddSingleton<IOutboxRelay, OutboxRelay>();
builder.Services.AddHostedService<OrderWorker>();
builder.Services.AddHostedService<OutboxRelayWorker>();

var host = builder.Build();


// Гарантируем наличие топика (best-effort)
try
{
    using var admin = new AdminClientBuilder(
        new AdminClientConfig { BootstrapServers = config.Kafka.BootstrapServers }).Build();
    await admin.CreateTopicsAsync(new[]
    {
        new TopicSpecification
        {
            Name = config.Kafka.Topic,
            NumPartitions = 3,
            ReplicationFactor = 1
        }
    });
}
catch (CreateTopicsException e) when (
    e.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
{ /* ok */ }

await host.RunAsync();