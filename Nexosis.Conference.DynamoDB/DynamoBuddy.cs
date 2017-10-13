using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

namespace Nexosis.Conference.DynamoDB
{
    public class DynamoBuddy
    {
        private static readonly AWSCredentials credentials = new EnvironmentVariablesAWSCredentials();
        private static readonly AmazonDynamoDBConfig config = new AmazonDynamoDBConfig
        {
            RegionEndpoint = RegionEndpoint.USEast2,
        };

        private readonly TextWriter output;

        public DynamoBuddy(TextWriter output)
        {
            this.output = output;
        }

        public async Task<int> CreateTables()
        {
            using (var client = new AmazonDynamoDBClient(credentials, config))
            {
                await Task.WhenAll(
                    CreateTable(client, "nexosis-conference-data", "series-key", "date"),
                    CreateTable(client, "nexosis-conference-grouped-data", "series-key", "start-date"));
            }

            return 0;
        }


        public async Task<int> DropTables()
        {
            using (var client = new AmazonDynamoDBClient(credentials, config))
            {
                await Task.WhenAll(
                    DropTable(client, "nexosis-conference-data"),
                    DropTable(client, "nexosis-conference-grouped-data"));
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

        public async Task<int> WriteData(int? datasetCount, int? rowCount, int? iterationCount, bool runContinuous, bool runParallel, bool usePages)
        {
            return 0;
        }

        public async Task<int> ReadData(int? datasetCount, int? iterationCount, bool runContinuous, bool runParallel, bool usePages)
        {
            return 0;
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
