using Microsoft.Extensions.CommandLineUtils;

namespace Nexosis.Conference.DynamoDB
{
    class Program
    {
        static readonly CommandLineApplication application = new CommandLineApplication();

        static void Main(string[] args)
        {
            try
            {
                var dynamoBuddy = new DynamoBuddy();

                application.HelpOption("-?|-h|--help");

                application.Command("create", command =>
                {
                    command.Description = "Creates DynamoDB tables for the application";
                    command.HelpOption("-?|-h|--help");

                    command.OnExecute(() => dynamoBuddy.CreateTables());
                });

                application.Command("write", command =>
                {
                    command.Description = "Writes to DynamoDB tables";
                    command.HelpOption("-?|-h|--help");

                    var datasetCount = command.Option("-d|--datasets", "count of datasets to write", CommandOptionType.SingleValue);
                    var rowCount = command.Option("-r|--rows", "count of rows per dataset to write", CommandOptionType.SingleValue);
                    var iterationCount = command.Option("-i|--iterations <count>", "count of iterations to execute", CommandOptionType.SingleValue);
                    var runContinuous = command.Option("-c|--continuous", "run continuously until a key is pressed", CommandOptionType.NoValue);
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
                    var runContinuous = command.Option("-c|--continuous", "run continuously until a key is pressed", CommandOptionType.NoValue);
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


                application.Execute(args);
            }
            catch (CommandParsingException cpe)
            {
                var errorConsole = AnsiConsole.GetError(false);

                errorConsole.WriteLine("");
                errorConsole.WriteLine("Error: " + cpe.Message);
            }
        }

        private static int? GetInt32Value(CommandOption option)
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
            return null;
        }
    }
}
