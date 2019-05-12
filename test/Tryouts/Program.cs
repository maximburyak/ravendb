
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace RavenDB4Tests.Tryouts
{
    public class QueryPerformance:RavenTestBase
    {
        
        public static void Main()
        {
            new QueryPerformance().QueryPerformanceTest(5000); 
        }
        public void QueryPerformanceTest(int kdocs)
        {
            var rng = new Random();
            using (var store = GetDocumentStore())            
            {

                Console.WriteLine(store.Urls.First());
                new DocsIndex().Execute(store);
                if (ShouldInitData(store))
                {
                    Console.WriteLine("Initializing data.");
                    InitializeData(store, kdocs);
                    Console.WriteLine("Data initialized.");
                }

                using (var session = store.OpenSession(new SessionOptions
                {
                    NoCaching = true
                }))
                {
                    var query = session.Query<IdHolder, DocsIndex>()
                        .Customize(x=>x.WaitForNonStaleResults(TimeSpan.FromMinutes(15)))
                        .Where(x=>x.Id =="2")
                        .ProjectInto<IdHolder>()
                        .Take(2);
                    var docs = query.ToList();
                }

                using (var session = store.OpenSession(new SessionOptions
                {
                    NoCaching = true
                }))
                {
                    var sw = Stopwatch.StartNew();
                    QueryStatistics stats;
                    var query = session.Query<IdHolder, DocsIndex>()
                        .Statistics(out stats)
                        .ProjectInto<IdHolder>()
                        .Take(1);
                    var docs = query.ToList();
                    Console.WriteLine($"unsorted {stats.TotalResults} records, Query: {stats.DurationInMs} ms, Wall time: {sw.Elapsed.TotalSeconds.ToString("###,##0.000")} seconds.");
                }

                WaitForUserToContinueTheTest(store,false);
            }
        }

        private static bool ShouldInitData(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var doc = session.Load<Doc>("doc/1");
                return doc == null;
            }
        }

        private static void InitializeData(IDocumentStore store, int kdocs)
        {
            var pos = 0;
            Console.WriteLine("Generating data.");
            var rng = new Random();
            using (var bulkInsert = store.BulkInsert())
            {
                for (int i = 1; i <= 1000 * kdocs; i++)
                {
                    ++pos;
                    if (pos % 10000 == 0)
                    {
                        Console.WriteLine($"Generated {pos}.");
                    }
                    bulkInsert.Store(new Doc { Id = "doc/" + i, IntVal = i, IntVals = Enumerable.Range(1, 50).ToDictionary(x => x.ToString(), x => i + x) });
                }
            }
            Console.WriteLine("Data generated.");
        }

        public class Doc
        {
            public string Id { get; set; }
            public int IntVal { get; set; }
            public Dictionary<string, int> IntVals { get; set; }
        }

        public class DocsIndex : AbstractIndexCreationTask<Doc>
        {
            public DocsIndex()
            {
                Map = docs =>
                    from doc in docs
                    select new
                    {
                        doc.Id,
                        doc.IntVal,
                        doc.IntVals,
                    };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        public class IdHolder
        {
            public string Id { get; set; }
        }
    }
}
