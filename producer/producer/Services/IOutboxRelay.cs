using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace producer.Services
{
    public interface IOutboxRelay
    {
        Task<int> ProcessBatchAsync(CancellationToken ct = default);
    }
}
