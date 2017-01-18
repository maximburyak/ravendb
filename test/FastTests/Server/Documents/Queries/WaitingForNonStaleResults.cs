﻿using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using Raven.NewClient.Operations.Databases.Indexes;
using Xunit;

namespace FastTests.Server.Documents.Queries
{
    [SuppressMessage("ReSharper", "ConsiderUsingConfigureAwait")]
    public class WaitingForNonStaleResults : RavenNewTestBase
    {
        [Fact]
        public async Task Cutoff_etag_usage()
        {
            long? lastEtagOfUser;

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User());
                    var entity = new User();
                    await session.StoreAsync(entity);

                    await session.StoreAsync(new Address());
                    await session.StoreAsync(new Address());

                    await session.SaveChangesAsync();

                    lastEtagOfUser = session.Advanced.GetEtagFor(entity);
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResultsAsOf(lastEtagOfUser)).OrderBy(x => x.Name).ToList();

                    Assert.Equal(2, users.Count);
                }
            }
        }

        [Fact]
        public async Task As_of_now_usage()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User());
                    await session.StoreAsync(new User());

                    await session.StoreAsync(new Address());
                    await session.StoreAsync(new Address());

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).OrderBy(x => x.Name).ToList();

                    Assert.Equal(2, users.Count);
                }
            }
        }

        [Fact]
        public void Throws_if_exceeds_timeout()
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Store(new Address());
                    session.Store(new Address());

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Throws<TimeoutException>(() =>
                        session.Query<Address>()
                        .Customize(x => x.WaitForNonStaleResultsAsOfNow(TimeSpan.FromMilliseconds(1)))
                        .OrderBy(x => x.City)
                        .ToList()
                    );
                }
            }
        }
    }
}