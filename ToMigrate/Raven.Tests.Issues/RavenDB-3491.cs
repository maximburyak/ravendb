using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Common.Dto;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3491 : RavenTestBase
    {
        private readonly TimeSpan waitForDocTimeout = TimeSpan.FromSeconds(20);

        [Fact]
        public void SubscribtionWithEtag()
        {
            using (var store = NewDocumentStore())
            {
                var us1 = new User { Id = "users/1", Name = "john", Age = 22 };
                var us2 = new User { Id = "users/2", Name = "KY", Age = 30 };
                var us3 = new User { Id = "users/3", Name = "BEN", Age = 30 };
                var us4 = new User { Id = "users/4", Name = "Hila", Age = 29 };
                var us5 = new User { Id = "users/5", Name = "Revital", Age = 34 };

                using (var session = store.OpenSession())
                {
                    session.Store(us1);
                    session.Store(us2);
                    session.Store(us3);
                    session.Store(us4);
                    session.Store(us5);
                    session.SaveChanges();

                    var user2Etag = session.Advanced.GetEtagFor(us2);
                    var id = store.Subscriptions.Create(new SubscriptionCriteria
                    {
                        StartEtag = user2Etag
                    });

                    var users = new List<RavenJObject>();

                    using (var subscription = store.AsyncSubscriptions.Open(new SubscriptionConnectionOptions(id)))
                    {
                        var docs = new BlockingCollection<RavenJObject>();
                        var keys = new BlockingCollection<string>();
                        var ages = new BlockingCollection<int>();

                        subscription.Subscribe(x => keys.Add(x[Constants.Metadata].Value<string>("@id")));
                        subscription.Subscribe(x => ages.Add(x.Value<int>("Age")));

                        subscription.Subscribe(docs.Add);

                        RavenJObject doc;
                        Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                        users.Add(doc);
                        Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                        users.Add(doc);
                        Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                        users.Add(doc);
                        var cnt = users.Count;
                        Assert.Equal(3, cnt);

                     
                        string key;
                        Assert.True(keys.TryTake(out key, waitForDocTimeout));
                        Assert.Equal("users/3", key);

                        Assert.True(keys.TryTake(out key, waitForDocTimeout));
                        Assert.Equal("users/4", key);

                        Assert.True(keys.TryTake(out key, waitForDocTimeout));
                        Assert.Equal("users/5", key);

                        int age;
                        Assert.True(ages.TryTake(out age, waitForDocTimeout));
                        Assert.Equal(30, age);

                        Assert.True(ages.TryTake(out age, waitForDocTimeout));
                        Assert.Equal(29, age);

                        Assert.True(ages.TryTake(out age, waitForDocTimeout));
                        Assert.Equal(34, age);
                    }
                }
            }
        }

        [Fact]
        public void StronglyTypedSubscribtionWithStartEtag()
        {
            using (var store = NewDocumentStore())
            {
                var us1 = new User { Id = "users/1", Name = "john", Age = 22 };
                var us2 = new User { Id = "users/2", Name = "KY", Age = 30 };
                var us3 = new User { Id = "users/3", Name = "BEN", Age = 30 };
                var us4 = new User { Id = "users/4", Name = "Hila", Age = 29 };
                var us5 = new User { Id = "users/5", Name = "Revital", Age = 34 };

                using (var session = store.OpenSession())
                {
                    session.Store(us1);
                    session.Store(us2);
                    session.Store(us3);
                    session.Store(us4);
                    session.Store(us5);
                    session.SaveChanges();

                    var user2Etag = session.Advanced.GetEtagFor(us2);
                    var id = store.Subscriptions.Create(new SubscriptionCriteria<User>
                    {
                        StartEtag = user2Etag
                    });

                    var users = new List<User>();

                    using (var subscription = store.AsyncSubscriptions.Open<User>(new SubscriptionConnectionOptions(id)))
                    {

                        var docs = new BlockingCollection<User>();
                        var keys = new BlockingCollection<string>();
                        var ages = new BlockingCollection<int>();

                        subscription.Subscribe(x => keys.Add(x.Id));
                        subscription.Subscribe(x => ages.Add(x.Age));

                        subscription.Subscribe(docs.Add);

                        User doc;
                        Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                        users.Add(doc);
                        Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                        users.Add(doc);
                        Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                        users.Add(doc);
                        var cnt = users.Count;
                        Assert.Equal(3, cnt);


                        string key;
                        Assert.True(keys.TryTake(out key, waitForDocTimeout));
                        Assert.Equal("users/3", key);

                        Assert.True(keys.TryTake(out key, waitForDocTimeout));
                        Assert.Equal("users/4", key);

                        Assert.True(keys.TryTake(out key, waitForDocTimeout));
                        Assert.Equal("users/5", key);

                        int age;
                        Assert.True(ages.TryTake(out age, waitForDocTimeout));
                        Assert.Equal(30, age);

                        Assert.True(ages.TryTake(out age, waitForDocTimeout));
                        Assert.Equal(29, age);

                        Assert.True(ages.TryTake(out age, waitForDocTimeout));
                        Assert.Equal(34, age);
                    }
                }
            }
        }

        [Fact]
        public void SubscribtionWithEtag_MultipleOpens()
        {
            using (var store = NewDocumentStore())
            {
                var us1 = new User { Id = "users/1", Name = "john", Age = 22 };
                var us2 = new User { Id = "users/2", Name = "KY", Age = 30 };
                var us3 = new User { Id = "users/3", Name = "BEN", Age = 30 };
                var us4 = new User { Id = "users/4", Name = "Hila", Age = 29 };
                var us5 = new User { Id = "users/5", Name = "Revital", Age = 34 };

                long subscriptionId;
                using (var session = store.OpenSession())
                {
                    session.Store(us1);
                    session.Store(us2);
                    session.Store(us3);
                    session.Store(us4);
                    session.Store(us5);
                    session.SaveChanges();

                    var user2Etag = session.Advanced.GetEtagFor(us2);
                    subscriptionId = store.Subscriptions.Create(new SubscriptionCriteria
                    {
                        StartEtag = user2Etag
                    });

                    var users = new List<RavenJObject>();

                    using (var subscription = store.AsyncSubscriptions.Open(new SubscriptionConnectionOptions(subscriptionId)))
                    {

                        var docs = new BlockingCollection<RavenJObject>();
                        var keys = new BlockingCollection<string>();
                        var ages = new BlockingCollection<int>();

                        subscription.Subscribe(x => keys.Add(x[Constants.Metadata].Value<string>("@id")));
                        subscription.Subscribe(x => ages.Add(x.Value<int>("Age")));

                        subscription.Subscribe(docs.Add);

                        RavenJObject doc;
                        Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                        users.Add(doc);
                        Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                        users.Add(doc);
                        Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                        users.Add(doc);
                        var cnt = users.Count;
                        Assert.Equal(3, cnt);

                       
                        string key;
                        Assert.True(keys.TryTake(out key, waitForDocTimeout));
                        Assert.Equal("users/3", key);

                        Assert.True(keys.TryTake(out key, waitForDocTimeout));
                        Assert.Equal("users/4", key);

                        Assert.True(keys.TryTake(out key, waitForDocTimeout));
                        Assert.Equal("users/5", key);

                        int age;
                        Assert.True(ages.TryTake(out age, waitForDocTimeout));
                        Assert.Equal(30, age);

                        Assert.True(ages.TryTake(out age, waitForDocTimeout));
                        Assert.Equal(29, age);

                        Assert.True(ages.TryTake(out age, waitForDocTimeout));
                        Assert.Equal(34, age);
                    }
                }
                using (var subscription = store.AsyncSubscriptions.Open(new SubscriptionConnectionOptions(subscriptionId)))
                {
                    var docs = new BlockingCollection<dynamic>();

                    subscription.Subscribe(o => docs.Add(o));

                    RavenJObject item;
                    Assert.False(docs.TryTake(out item,TimeSpan.FromMilliseconds(250)));

                }
            }
        }
    }
}
