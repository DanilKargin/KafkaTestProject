using Dapper;
using Microsoft.Extensions.Logging;
using producer.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace producer.Services
{
    public class OrderService : IOrderService
    {
        private readonly Database _db;
        private readonly ILogger<OrderService> _log;
        private readonly Random _rnd = new();

        public OrderService(Database db, ILogger<OrderService> log)
        {
            _db = db;
            _log = log;
        }

        public async Task<Guid> CreateOrderAsync(CancellationToken ct = default)
        {
            var orderId = Guid.NewGuid();
            var customerId = Guid.NewGuid();
            var amount = Math.Round((decimal)(_rnd.NextDouble() * 1000), 2);
            var eventId = Guid.NewGuid();

            var payload = JsonSerializer.Serialize(new
            {
                id = eventId,
                timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                payload = new { orderId, customerId, amount, action = "RESERVE" }
            });

            await using var conn = await _db.OpenAsync(ct);
            await using var tx = await conn.BeginTransactionAsync(ct);

            await conn.ExecuteAsync(@"
            INSERT INTO orders (id, customer_id, amount, status)
            VALUES (@id, @customerId, @amount, 'CREATED')",
                new { id = orderId, customerId, amount }, tx);

            await conn.ExecuteAsync(@"
            INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
            VALUES ('Order', @aggregateId, 'OrderReserveRequested', @payload::jsonb)",
                new { aggregateId = orderId, payload }, tx);

            await tx.CommitAsync(ct);

            _log.LogInformation(
                "Создан заказ {OrderId}, сумма {Amount}, event {EventId} → outbox (PENDING)",
                orderId, amount, eventId);

            return orderId;
        }
    }
}
