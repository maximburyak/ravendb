using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using SlowTests.Voron.Compaction;
using Tests.Infrastructure;
using Voron;
using Voron.Impl.Journal;
using Xunit;

namespace Tryouts2
{

    public class ReproTest : RavenTestBase
    {        
        public async Task CanCompactDatabase(string dbName, int seed)
        {
            var path = NewDataPath(dbName + Guid.NewGuid().ToString());
            using (var store = GetDocumentStore(new Options
            {
                Path = path,
                DeleteDatabaseOnDispose = false                
            }, caller: dbName))
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                for (int i = 0; i < 3; i++)
                {
                    await store.Operations.Send(new PatchByQueryOperation(new IndexQuery
                    {
                        Query = @"FROM Orders UPDATE { put(""orders/"", this); } "
                    })).WaitForCompletionAsync(TimeSpan.FromSeconds(300));
                }

                WaitForIndexing(store);

                var deleteOperation = store.Operations.Send(new DeleteByQueryOperation(new IndexQuery() { Query = "FROM orders" }));
                await deleteOperation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                List<string> IDs;
                var random = new Random(seed);
                var skip = 0;
                using (var session = store.OpenSession())
                {
                    session.Advanced.MaxNumberOfRequestsPerSession = 10_000;
                    var orders = session.Advanced.DocumentQuery<Orders.Order>().RandomOrdering(seed.ToString()).Select(x => x.Id).ToList();

                    while (skip < orders.Count){
                        var toTake = random.Next(0, 1000);

                        var toDelete = orders.Skip(skip).Take(toTake);

                        foreach (var item in toDelete)
                        {
                            session.Delete(item);
                        }

                        session.SaveChanges();
                        //GlobalFlushingBehavior.GlobalFlusher.Value.
                        skip += toTake;
                    }
                }




                var oldSize = StorageCompactionTestsSlow.GetDirSize(new DirectoryInfo(path));

               // WriteAheadJournal._globalDic.Clear();

                var compactOperation = store.Maintenance.Server.Send(new CompactDatabaseOperation(new CompactSettings
                {
                    DatabaseName = store.Database,
                    Documents = true,
                    Indexes = new[] { "Orders/ByCompany", "Orders/Totals" }
                }));
                await compactOperation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                var newSize = StorageCompactionTestsSlow.GetDirSize(new DirectoryInfo(path));

                Assert.True(oldSize > newSize);
            }
        }
    }
    public class SecondTest : RavenTestBase
    {
        public void Do()
        {

            //var directoryPath = @"C:\\GitRepositories\\ravenv4.0\\Tryouts2\\bin\\Release\\netcoreapp2.1\\Databases";
            //var dir = Directory.EnumerateDirectories(directoryPath);
            //foreach (var innerDir in dir)
            //{
                //Console.WriteLine(innerDir);
                using (var store = GetDocumentStore(new Options
                {
                    Path = @"C:\\GitRepositories\\ravenv4.0\\Tryouts2\\bin\\Release\\netcoreapp2.1\\Databases\\d93e9cd3-6635-4a53-aad3-0058f6688ea6738b27d0-db37-42c7-a8f2-e5efc57f63e6.0-1 - Copy"

                }))
                {

                }
            //}
        }
    }
    class Program
    {
        public static void Main(string[] args)
        {
            //new SecondTest().Do();
            //return;
            var random = new Random();
            var curSeed = random.Next();
            var failCount = 0;
            //467771263
            for (var i=0; i<1000; i++)
             {
              //  WriteAheadJournal._globalDic.Clear();
                 Console.WriteLine(i);
                 var failed = false;
                //WriteAheadJournal.val.Clear();
                string dbName = Guid.NewGuid().ToString();
                try
                {
                    using (var test = new ReproTest())
                    {
                        try
                        {


                            test.CanCompactDatabase(dbName, random.Next()).Wait();

                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex);


                            if (ex.ToString().Contains("Invalid checksum for page"))
                            {
                                failed = true;
                            }
                            failCount++;
                        }

                        if (failed)
                        {
                            
                            Console.WriteLine("failure " + dbName);
                            Debugger.Launch();

                        }
                        else
                        {

                            Console.WriteLine("success");
                        }
                    }
                }
                catch (Exception)
                {

                    //throw;
                }

            }
        }
    }
}
