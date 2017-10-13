using System.Threading.Tasks;

namespace Nexosis.Conference.DynamoDB
{
    public class DynamoBuddy
    {
        public async Task<int> CreateTables()
        {
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
    }
}
