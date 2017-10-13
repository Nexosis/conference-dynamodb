using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

namespace Nexosis.Conference.DynamoDB
{
    public class DynamoBuddy
    {
        private const string tableName = "nexosis-conference-data";
        private const string groupedTableName = "nexosis-conference-data-grouped";

        private static readonly AWSCredentials credentials = new EnvironmentVariablesAWSCredentials();
        private static readonly AmazonDynamoDBConfig config = new AmazonDynamoDBConfig
        {
            RegionEndpoint = RegionEndpoint.USEast2,
        };

        private readonly TextWriter output;
        private readonly Stopwatch stopwatch;

        private int totalCount = 0;

        public DynamoBuddy(TextWriter output)
        {
            this.output = output;

            stopwatch = new Stopwatch();
            stopwatch.Start();
        }

        public async Task<int> CreateTables()
        {
            using (var client = new AmazonDynamoDBClient(credentials, config))
            {
                await Task.WhenAll(
                    CreateTable(client, tableName, "seriesKey", "date"),
                    CreateTable(client, groupedTableName, "seriesKey", "startDate"));
            }

            return 0;
        }


        public async Task<int> DropTables()
        {
            using (var client = new AmazonDynamoDBClient(credentials, config))
            {
                await Task.WhenAll(
                    DropTable(client, tableName),
                    DropTable(client, groupedTableName));
            }

            return 0;
        }

        public async Task<int> ScaleTable(string tableName, int reads, int writes)
        {
            using (var client = new AmazonDynamoDBClient(credentials, config))
            {
                var request = new UpdateTableRequest
                {
                    TableName = tableName,
                    ProvisionedThroughput = new ProvisionedThroughput
                    {
                        ReadCapacityUnits = reads,
                        WriteCapacityUnits = writes
                    }
                };

                try
                {
                    await output.WriteLineAsync($"Scaling table: {tableName}");
                    await client.UpdateTableAsync(request);
                    await output.WriteLineAsync($"Scaled table: {tableName}");
                }
                catch (AmazonDynamoDBException ex)
                {
                    await output.WriteLineAsync(ex.Message);
                }
            }

            return 0;
        }

        public async Task<int> ReadData(int? datasetCount, int? rowCount, bool runParallel, bool useGroupedTable)
        {
            await ReadSeries(rowCount, $"series000", useGroupedTable);

            await output.WriteLineAsync($"Read {totalCount} total records in {stopwatch.Elapsed}");

            return 0;
        }
        private async Task ReadSeries(int? rowCount, string seriesKey, bool useGroupedTable)
        {
            using (var client = new AmazonDynamoDBClient(credentials, config))
            {
                var readFromTableName = useGroupedTable
                    ? groupedTableName
                    : tableName;

                var count = 0;

                Dictionary<string, AttributeValue> lastReadKey = null;

                while (true)
                {
                    var request = new QueryRequest
                    {
                        TableName = readFromTableName,
                        ConsistentRead = false,
                        ExclusiveStartKey = lastReadKey,
                        KeyConditionExpression = "seriesKey=:seriesKey",
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                        {
                            [":seriesKey"] = new AttributeValue { S = seriesKey }
                        },
                    };

                    try
                    {
                        var response = await client.QueryAsync(request);

                        count += response.Items.Count;
                        Interlocked.Add(ref totalCount, response.Items.Count);

                        await output.WriteLineAsync($"Read {count} records from dataset '{seriesKey}' in: {tableName}");

                        if (response.LastEvaluatedKey.Any() && count <= rowCount.GetValueOrDefault(int.MaxValue))
                            lastReadKey = response.LastEvaluatedKey;
                        else
                            break;
                    }
                    catch (AmazonDynamoDBException ex)
                    {
                        await output.WriteLineAsync(ex.Message);
                    }
                }
            }
        }

        public async Task<int> WriteData(int? datasetCount, int? rowCount, bool runContinuous, bool runParallel, bool useGroupedTable)
        {
            await WriteSeries(rowCount, runContinuous, useGroupedTable, $"series000");

            await output.WriteLineAsync($"Wrote {totalCount} total records in {stopwatch.Elapsed}");

            return 0;
        }
        private async Task WriteSeries(int? rowCount, bool runContinuous, bool useGroupedTable, string seriesKey)
        {
            using (var client = new AmazonDynamoDBClient(credentials, config))
            {
                var dataSet = new DataSet(seriesKey);

                var writeToTableName = useGroupedTable
                    ? groupedTableName
                    : tableName;

                var records = useGroupedTable
                    ? dataSet.GetRowGroups(rowCount, runContinuous).Select(CreateWriteRequest)
                    : dataSet.GetRows(rowCount, runContinuous).Select(CreateWriteRequest);

                var count = 0;

                foreach (var batch in records.Batch(25)) // DynamoDB supports writing 25 records at a time
                {
                    var requestItems = new Dictionary<string, List<WriteRequest>>
                    {
                        [writeToTableName] = batch
                    };

                    while (requestItems.Any())
                    {
                        var request = new BatchWriteItemRequest()
                        {
                            RequestItems = requestItems
                        };

                        try
                        {
                            var response = await client.BatchWriteItemAsync(request);

                            // Attempt to write unprocessed items again
                            requestItems = response.UnprocessedItems;
                        }
                        catch (AmazonDynamoDBException ex)
                        {
                            await output.WriteLineAsync(ex.Message);
                        }
                    }

                    count += batch.Count;
                    Interlocked.Add(ref totalCount, batch.Count);

                    await output.WriteLineAsync($"Wrote {count} records from dataset '{dataSet.SeriesKey}' to: {tableName}");
                }
            }
        }

        private static WriteRequest CreateWriteRequest(Row row)
        {
            return new WriteRequest
            {
                PutRequest = new PutRequest
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        ["seriesKey"] = new AttributeValue { S = row.SeriesKey },
                        ["date"] = new AttributeValue { S = row.Date.ToString("o") },
                        ["target"] = new AttributeValue { N = row.Target.ToString() }
                    }
                }
            };
        }
        private static WriteRequest CreateWriteRequest(RowGroup rowGroup)
        {
            return new WriteRequest
            {
                PutRequest = new PutRequest
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        ["seriesKey"] = new AttributeValue { S = rowGroup.SeriesKey },
                        ["startDate"] = new AttributeValue { S = rowGroup.StartDate.ToString("o") },
                        ["targets"] = new AttributeValue
                        {
                            M = rowGroup.Targets.ToDictionary(
                                kv => kv.Key.ToString("o"),
                                kv => new AttributeValue { N = kv.Value.ToString() })
                        }
                    }
                }
            };
        }

        private async Task CreateTable(AmazonDynamoDBClient client, string tableName, string hashKeyName, string rangeKeyName)
        {
            var request = new CreateTableRequest
            {
                TableName = tableName,
                KeySchema = new List<KeySchemaElement>
                {
                    new KeySchemaElement { AttributeName = hashKeyName, KeyType = KeyType.HASH },
                    new KeySchemaElement { AttributeName = rangeKeyName, KeyType = KeyType.RANGE },
                },
                AttributeDefinitions = new List<AttributeDefinition>
                {
                    new AttributeDefinition { AttributeName = hashKeyName, AttributeType = ScalarAttributeType.S },
                    new AttributeDefinition { AttributeName = rangeKeyName, AttributeType = ScalarAttributeType.S },
                },
                ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = 10,
                    WriteCapacityUnits = 10
                },
            };

            try
            {
                await output.WriteLineAsync($"Creating table: {tableName}");
                await client.CreateTableAsync(request);
                await output.WriteLineAsync($"Created table: {tableName}");
            }
            catch (AmazonDynamoDBException ex)
            {
                await output.WriteLineAsync(ex.Message);
            }
        }

        private async Task DropTable(AmazonDynamoDBClient client, string tableName)
        {
            try
            {
                await output.WriteLineAsync($"Dropping table: {tableName}");
                await client.DeleteTableAsync(tableName);
                await output.WriteLineAsync($"Dropped table: {tableName}");

            }
            catch (AmazonDynamoDBException ex)
            {
                await output.WriteLineAsync(ex.Message);
            }
        }
    }
}
