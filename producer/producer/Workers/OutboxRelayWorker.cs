using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using producer.Configuration;
using producer.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace producer.Workers
{
    public class OutboxRelayWorker : BackgroundService
    {
        private readonly IOutboxRelay _relay;
        private readonly OutboxRelayOptions _options;
        private readonly ILogger<OutboxRelayWorker> _log;

        public OutboxRelayWorker(
            IOutboxRelay relay,
            AppConfig config,
            ILogger<OutboxRelayWorker> log)
        {
            _relay = relay;
            _options = config.OutboxRelay;
            _log = log;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _log.LogInformation("OutboxRelayWorker старт.");
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var count = await _relay.ProcessBatchAsync(stoppingToken);
                    if (count == 0)
                        await Task.Delay(_options.PollIntervalMs, stoppingToken);
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    _log.LogError(ex, "Ошибка в OutboxRelayWorker");
                    await Task.Delay(2000, stoppingToken);
                }
            }
        }
    }
}
