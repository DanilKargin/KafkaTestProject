using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace producer.Domain
{
    public class OutboxMessage
    {
        public long Id { get; set; }
        public string AggregateType { get; set; } = "";
        public Guid AggregateId { get; set; }
        public string EventType { get; set; } = "";
        public string Payload { get; set; } = "";
        public DateTime CreatedAt { get; set; }
        public DateTime? PublishedAt { get; set; }
        public string Status { get; set; } = "";
    }
}
