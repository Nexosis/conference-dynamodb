using System;
using System.Collections.Generic;
using System.Linq;

namespace Nexosis.Conference.DynamoDB
{
    class DataSet
    {
        private const int defaultRowCount = 500;

        private static Random random = new Random();

        public string SeriesKey { get; }

        public DataSet(string seriesKey)
        {
            SeriesKey = seriesKey;
        }

        public IEnumerable<Row> GetRows(int? rowCount, bool runContinuous)
        {
            var startDate = new DateTimeOffset(2010, 1, 1, 0, 0, 0, TimeSpan.Zero);

            var indexes = runContinuous
                ? Enumerable.Range(0, int.MaxValue)
                : Enumerable.Range(0, rowCount.GetValueOrDefault(defaultRowCount));

            foreach (var i in indexes)
            {
                yield return new Row
                {
                    SeriesKey = SeriesKey,
                    Date = startDate.AddDays(i),
                    Target = (i + random.Next(500))
                };
            }
        }
        public IEnumerable<RowGroup> GetRowGroups(int? rowCount, bool runContinuous)
        {
            var groupStartDate = DateTimeOffset.MinValue;
            var targets = new Dictionary<DateTimeOffset, int>();

            foreach (var row in GetRows(rowCount, runContinuous))
            {
                var rowStartDate = row.Date.AddDays(1 - row.Date.Day);
                if (rowStartDate > groupStartDate)
                {
                    if (targets.Any())
                    {
                        // Create and return a row group
                        yield return new RowGroup
                        {
                            SeriesKey = SeriesKey,
                            StartDate = groupStartDate,
                            Targets = targets
                        };
                    }

                    // Start creating a new group
                    groupStartDate = rowStartDate;
                    targets = new Dictionary<DateTimeOffset, int>();
                }

                // Add the row to the current group of targets
                targets.Add(row.Date, row.Target);
            }

            if (targets.Any())
            {
                // Create and return the final row group
                yield return new RowGroup
                {
                    SeriesKey = SeriesKey,
                    StartDate = groupStartDate,
                    Targets = targets
                };
            }
        }
    }
}
