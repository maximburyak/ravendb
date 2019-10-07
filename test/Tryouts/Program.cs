using System;
using System.Diagnostics;
using System.Threading.Tasks;
using SlowTests.Client.Counters;
using SlowTests.Cluster;
using SlowTests.Issues;
using SlowTests.Server;
using SlowTests.Voron;
using StressTests.Cluster;

namespace Tryouts
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            new ServerStoreBackup().Backup();
        }
    }
}
