using Confluent.Kafka;
using producer.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace producer.Infrastructure
{
    public static class KafkaProducerFactory
    {
        public static IProducer<string, string> Create(KafkaOptions options)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = options.BootstrapServers,
                Acks = Acks.All,
                EnableIdempotence = true,
                MessageSendMaxRetries = 5,
                MaxInFlight = 5
            };
            return new ProducerBuilder<string, string>(config).Build();
        }
    }
}
