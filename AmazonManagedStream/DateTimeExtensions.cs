using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AmazonManagedStream
{
    public static class DateTimeExtensions
    {
        public static long ToUnixTimeMilliSeconds(this DateTime dateTime)
        {
            DateTimeOffset dto = new DateTimeOffset(dateTime.ToUniversalTime());
            return dto.ToUnixTimeMilliseconds();
        }
    }
}
