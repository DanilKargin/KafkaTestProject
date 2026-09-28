using Confluent.Kafka;
using Dapper;
using Microsoft.Extensions.Logging;
using producer.Configuration;
using producer.Domain;
using producer.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace producer.Services
{
    public class OutboxRelay : IOutboxRelay
    {
        private readonly Database _db;
        private readonly IProducer<string, string> _producer;
        private readonly KafkaOptions _kafka;
        private readonly OutboxRelayOptions _options;
        private readonly ILogger<OutboxRelay> _log;

        public OutboxRelay(
            Database db,
            IProducer<string, string> producer,
            AppConfig config,
            ILogger<OutboxRelay> log)
        {
            _db = db;
            _producer = producer;
            _kafka = config.Kafka;
            _options = config.OutboxRelay;
            _log = log;
        }

        public async Task<int> ProcessBatchAsync(CancellationToken ct = default)
        {
            await using var conn = await _db.OpenAsync(ct);
            await using var tx = await conn.BeginTransactionAsync(ct);

            // Лочим пачку через SKIP LOCKED
            var rows = (await conn.QueryAsync<OutboxMessage>(@"
            SELECT id, aggregate_type, aggregate_id, event_type,
                   payload::text AS payload, created_at, published_at, status
            FROM outbox
            WHERE status = 'PENDING'
            ORDER BY created_at
            LIMIT @limit
            FOR UPDATE SKIP LOCKED",
                new { limit = _options.BatchSize }, tx)).ToList();

            if (rows.Count == 0)
            {
                await tx.RollbackAsync(ct);
                return 0;
            }

            var published = 0;
            foreach (var row in rows)
            {
                try
                {
                    await _producer.ProduceAsync(_kafka.Topic, new Message<string, string>
                    {
                        Key = row.Id.ToString(),
                        Value = row.Payload
                    }, ct);

                    await conn.ExecuteAsync(@"
                    UPDATE outbox
                    SET status = 'PUBLISHED', published_at = NOW()
                    WHERE id = @id", new { id = row.Id }, tx);

                    published++;
                    _log.LogInformation(
                        "→ Kafka outbox.id={Id} aggregate={AggregateId} type={EventType}",
                        row.Id, row.AggregateId, row.EventType);
                }
                catch (ProduceException<string, string> ex)
                {
                    _log.LogError(ex, "Ошибка публикации outbox.id={Id}", row.Id);
                    // оставляем PENDING для ретрая
                }
            }

            await tx.CommitAsync(ct);
            return published;
        }
    }
}
