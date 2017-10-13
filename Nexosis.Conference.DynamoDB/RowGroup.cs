using System;
using System.Collections.Generic;

namespace Nexosis.Conference.DynamoDB
{
    public class RowGroup
    {
        public string SeriesKey { get; set; }
        public DateTimeOffset StartDate { get; set; }
        public Dictionary<DateTimeOffset, int> Targets { get; set; }
    }
}
