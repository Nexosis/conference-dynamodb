using Microsoft.Extensions.CommandLineUtils;

namespace Nexosis.Conference.DynamoDB
{
    class Program
    {
        static readonly CommandLineApplication application = new CommandLineApplication();

        static int Main(string[] args)
        {
            try
            {
                var dynamoBuddy = new DynamoBuddy(application.Out);

                application.Name = "Nexosis Conference DynamoDB Buddy";
                application.Description = "Demonstrates various problems and remedies when interacting with Amazon DynamoDB";
                application.HelpOption("-?|-h|--help");

                application.Command("create", command =>
                {
                    command.Description = "Creates DynamoDB tables for the application";
                    command.HelpOption("-?|-h|--help");

                    command.OnExecute(() => dynamoBuddy.CreateTables());
                });

                application.Command("drop", command =>
                {
                    command.Description = "Drops DynamoDB tables created for the application";
                    command.HelpOption("-?|-h|--help");

                    command.OnExecute(() => dynamoBuddy.DropTables());
                });

                application.Command("scale", command =>
                {
                    command.Description = "Scales DynamoDB tables to the specified values";
                    command.HelpOption("-?|-h|--help");

                    var table = command.Option("-t|--table", "name of the table to scale", CommandOptionType.SingleValue);
                    var reads = command.Option("-r|--reads", "read capacity units to scale to", CommandOptionType.SingleValue);
                    var writes = command.Option("-w|--writes", "write capacity units to scale to", CommandOptionType.SingleValue);

                    command.OnExecute(() => dynamoBuddy.ScaleTable(
                        table.Value(),
                        GetInt32Value(reads, true).Value,
                        GetInt32Value(writes, true).Value));
                });

                application.Command("write", command =>
                {
                    command.Description = "Writes to DynamoDB tables";
                    command.HelpOption("-?|-h|--help");

                    var datasetCount = command.Option("-d|--datasets", "count of datasets to write", CommandOptionType.SingleValue);
                    var rowCount = command.Option("-r|--rows", "count of rows per dataset to write", CommandOptionType.SingleValue);
                    var iterationCount = command.Option("-i|--iterations <count>", "count of iterations to execute", CommandOptionType.SingleValue);
                    var runContinuous = command.Option("-c|--continuous", "run continuously until the application is shut down", CommandOptionType.NoValue);
                    var runParallel = command.Option("-p|--parallel", "write each dataset in parallel", CommandOptionType.NoValue);
                    var usePages = command.Option("-g|--group", "group data into pages", CommandOptionType.NoValue);

                    command.OnExecute(() => dynamoBuddy.WriteData(
                        GetInt32Value(datasetCount),
                        GetInt32Value(rowCount),
                        GetInt32Value(iterationCount),
                        runContinuous.HasValue(),
                        runParallel.HasValue(),
                        usePages.HasValue()
                    ));
                });

                application.Command("read", command =>
                {
                    command.Description = "Reads from DynamoDB tables";
                    command.HelpOption("-?|-h|--help");

                    var datasetCount = command.Option("-d|--datasets", "count of datasets to read", CommandOptionType.SingleValue);
                    var iterationCount = command.Option("-i|--iterations <count>", "count of iterations to execute", CommandOptionType.SingleValue);
                    var runContinuous = command.Option("-c|--continuous", "run continuously until the application is shut down", CommandOptionType.NoValue);
                    var runParallel = command.Option("-p|--parallel", "read each dataset in parallel", CommandOptionType.NoValue);
                    var usePages = command.Option("-g|--group", "group data into pages", CommandOptionType.NoValue);

                    command.OnExecute(() => dynamoBuddy.ReadData(
                        GetInt32Value(datasetCount),
                        GetInt32Value(iterationCount),
                        runContinuous.HasValue(),
                        runParallel.HasValue(),
                        usePages.HasValue()
                    ));
                });

                return application.Execute(args);
            }
            catch (CommandParsingException cpe)
            {
                application.Error.WriteLine("");
                application.Error.WriteLine("Error: " + cpe.Message);

                return -1;
            }
        }

        private static int? GetInt32Value(CommandOption option, bool require = true)
        {
            if (option.HasValue())
            {
                if (int.TryParse(option.Value(), out int value))
                {
                    return value;
                }
                else
                {
                    throw new CommandParsingException(application, $"Option '{option.Template}' was supplied with a non-integer value");
                }
            }

            // Option isn't specified
            if (require)
                throw new CommandParsingException(application, $"Option '{option.Template}' must be supplied with an integer value");
            else
                return null;
        }
    }
}
