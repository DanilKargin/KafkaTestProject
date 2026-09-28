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
    public class OrderWorker : BackgroundService
    {
        private readonly IOrderService _orderService;
        private readonly OrderServiceOptions _options;
        private readonly ILogger<OrderWorker> _log;
        private readonly Random _rnd = new();

        public OrderWorker(
            IOrderService orderService,
            AppConfig config,
            ILogger<OrderWorker> log)
        {
            _orderService = orderService;
            _options = config.OrderService;
            _log = log;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            //_log.LogInformation("OrderWorker старт.");
            //while (!stoppingToken.IsCancellationRequested)
            //{
            //    var delay = _rnd.Next(_options.MinDelaySeconds, _options.MaxDelaySeconds + 1);
            //    try
            //    {
            //        await Task.Delay(TimeSpan.FromSeconds(delay), stoppingToken);
            //        await _orderService.CreateOrderAsync(stoppingToken);
            //    }
            //    catch (OperationCanceledException) { break; }
            //    catch (Exception ex)
            //    {
            //        _log.LogError(ex, "Ошибка в OrderWorker");
            //    }
            //}
        }
    }
}
