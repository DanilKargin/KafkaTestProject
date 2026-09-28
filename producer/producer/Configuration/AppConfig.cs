using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace producer.Configuration
{
    public class AppConfig
    {
        public KafkaOptions Kafka { get; set; } = new();
        public DatabaseOptions Database { get; set; } = new();
        public OrderServiceOptions OrderService { get; set; } = new();
        public OutboxRelayOptions OutboxRelay { get; set; } = new();
    }

    public class KafkaOptions
    {
        public string BootstrapServers { get; set; } = "localhost:9092";
        public string Topic { get; set; } = "demo-topic";
    }

    public class DatabaseOptions
    {
        public string ConnectionString { get; set; } = "";
    }

    public class OrderServiceOptions
    {
        public int MinDelaySeconds { get; set; } = 1;
        public int MaxDelaySeconds { get; set; } = 5;
    }

    public class OutboxRelayOptions
    {
        public int PollIntervalMs { get; set; } = 1000;
        public int BatchSize { get; set; } = 50;
    }
}
