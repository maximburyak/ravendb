using System;
using System.Diagnostics;
using System.Threading.Tasks;
using SlowTests.Client.Counters;
using SlowTests.Cluster;
using SlowTests.Issues;
using SlowTests.Server;
using SlowTests.Voron;
using StressTests.Cluster;
using Tests.Infrastructure;

namespace Tryouts
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            using var helper = new ConsoleTestOutputHelper();
            using (var test = new ServerStoreBackup(helper))
            {
                await test.Backup();
            }
        }
    }
}
