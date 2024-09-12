using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AmazonManagedStream
{
    public interface IKafkaProducerService
    {
        Task KafkaConfiguration();
    }
}
