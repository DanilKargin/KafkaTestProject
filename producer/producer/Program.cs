using Confluent.Kafka;
using System.Text.Json;

var bootstrap = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP") ?? "localhost:9092";

var config = new ProducerConfig
{
    BootstrapServers = bootstrap,
    Acks = Acks.All
};

using var producer = new ProducerBuilder<string, string>(config).Build();
var random = new Random();

Console.WriteLine($"[PRODUCER] Старт. Bootstrap: {bootstrap}");

// Небольшая пауза, чтобы Kafka успела подняться
await Task.Delay(5000);

while (true)
{
    var msgObj = new
    {
        id = Guid.NewGuid().ToString(),
        timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
        payload = $"Message-{random.Next(1000)}"
    };
    string json = JsonSerializer.Serialize(msgObj);

    try
    {
        var result = await producer.ProduceAsync("demo-topic",
            new Message<string, string> { Value = json });

        Console.WriteLine($"[SENT] id={msgObj.id}, payload={msgObj.payload}, " +
                          $"partition={result.Partition.Value}, offset={result.Offset.Value}");
    }
    catch (ProduceException<string, string> e)
    {
        Console.WriteLine($"[ERROR] {e.Error.Reason}");
    }

    await Task.Delay(2000);
}