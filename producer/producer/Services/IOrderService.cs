using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace producer.Services
{
    public interface IOrderService
    {
        Task<Guid> CreateOrderAsync(CancellationToken ct = default);
    }
}
