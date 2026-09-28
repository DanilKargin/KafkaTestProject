using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace producer.Domain
{
    public record Order(
         Guid Id,
         Guid CustomerId,
         decimal Amount,
         string Status,
         DateTimeOffset CreatedAt);
}
