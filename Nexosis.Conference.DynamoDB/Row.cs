using System;

namespace Nexosis.Conference.DynamoDB
{
    public class Row
    {
        public string SeriesKey { get; set; }
        public DateTimeOffset Date { get; set; }
        public int Target { get; set; }
    }
}
