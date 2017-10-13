using System.Collections.Generic;
using System.Linq;

namespace Nexosis.Conference.DynamoDB
{
    static class Extensions
    {
        public static IEnumerable<List<T>> Batch<T>(this IEnumerable<T> source, int batchSize)
        {
            var batch = new List<T>(batchSize);

            foreach (var item in source)
            {
                if (batch.Count >= batchSize)
                {
                    // Return the batch if it is full
                    yield return batch;

                    // Start creating a new batch
                    batch = new List<T>(batchSize);
                }

                // Add the item to the batch
                batch.Add(item);
            }

            if (batch.Any())
            {
                // Return the last batch if necessary
                yield return batch;
            }
        }
    }
}
