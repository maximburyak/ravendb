﻿using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Common;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Indexes
{
    public class IndexReplacementTest : RavenTest
    {

        private class Person
        {
            public string Id { get; set; }

            public string FirstName { get; set; }

            public string LastName { get; set; }
        }

        private class OldIndex : AbstractIndexCreationTask<Person>
        {
            public OldIndex()
            {
                Map = persons => from person in persons select new { person.FirstName };
            }
        }

        private class NewIndex : AbstractIndexCreationTask<Person>
        {
            public NewIndex()
            {
                Map = persons => from person in persons select new { person.FirstName, person.LastName };
            }
        }

        [Fact]
        public void ReplaceAfterNonStale()
        {
            using (var store = NewDocumentStore(runInMemory: false))
            {
                IndexCreation.CreateIndexes(new CompositionContainer(new TypeCatalog(typeof(OldIndex))), store);

                WaitForIndexing(store);

                var e = Assert.Throws<InvalidOperationException>(() =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Query<Person, OldIndex>()
                            .Count(x => x.LastName == "Doe");
                    }
                });

                Assert.Contains("The field 'LastName' is not indexed, cannot query on fields that are not indexed", e.InnerException.Message);

                IndexCreation.SideBySideCreateIndexes(new CompositionContainer(new TypeCatalog(typeof(NewIndex))), store);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Person, OldIndex>()
                                       .Count(x => x.LastName == "Doe");

                    Assert.Equal(0, count);
                }
            }
        }

    }
}
