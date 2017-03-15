// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3484.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Threading;
using Raven.Client.Documents.Exceptions.Subscriptions;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Extensions;
using FastTests.Server.Documents.Notifications;

namespace FastTests.Client.Subscriptions
{
    public class RavenDB_3484 : RavenTestBase
    {
        private readonly TimeSpan waitForDocTimeout = TimeSpan.FromSeconds(20);

        [Fact]
        public void OpenIfFree_ShouldBeDefaultStrategy()
        {
            Assert.Equal(SubscriptionOpeningStrategy.OpenIfFree, new SubscriptionConnectionOptions(1).Strategy);
        }

        [Fact]
        public void ShouldRejectWhen_OpenIfFree_StrategyIsUsed()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }
                var id = store.Subscriptions.Create(new SubscriptionCriteria<User>());
                var subscription = store.Subscriptions.Open(new SubscriptionConnectionOptions(id));
                subscription.Subscribe(x => { });
                subscription.Start();

                var throwingSubscription = store.Subscriptions.Open(new SubscriptionConnectionOptions(id)
                {
                    Strategy = SubscriptionOpeningStrategy.OpenIfFree
                });
                throwingSubscription.Subscribe(x => { });
                
                Assert.Throws<SubscriptionInUseException>(() => throwingSubscription.Start());
            }
        }

        [Fact]
        public void ShouldReplaceActiveClientWhen_TakeOver_StrategyIsUsed()
        {
            using (var store = GetDocumentStore())
            {
                var id = store.Subscriptions.Create(new SubscriptionCriteria<User>());

                const int numberOfClients = 4;

                var subscriptions = new Subscription<User>[numberOfClients];
                var items = new BlockingCollection<User>[numberOfClients];

                for (int i = 0; i < numberOfClients; i++)
                {
                    subscriptions[i] = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(id)
                    {
                        Strategy = i > 0 ? SubscriptionOpeningStrategy.TakeOver : SubscriptionOpeningStrategy.OpenIfFree
                    });

                    items[i] = new BlockingCollection<User>();

                    subscriptions[i].Subscribe(items[i].Add);

                    subscriptions[i].Start();

                    bool batchAcknowledged = false;

                    subscriptions[i].AfterAcknowledgment += () => batchAcknowledged = true;

                    using (var s = store.OpenSession())
                    {
                        s.Store(new User());
                        s.Store(new User());

                        s.SaveChanges();
                    }

                    User user;

                    Assert.True(items[i].TryTake(out user, waitForDocTimeout));
                    Assert.True(items[i].TryTake(out user, waitForDocTimeout));

                    SpinWait.SpinUntil(() => batchAcknowledged, TimeSpan.FromSeconds(5)); // let it acknowledge the processed batch before we open another subscription

                    if (i > 0)
                    {
                        Assert.False(items[i - 1].TryTake(out user, TimeSpan.FromSeconds(2)));
                        Assert.True(SpinWait.SpinUntil(() => subscriptions[i - 1].IsConnectionClosed, TimeSpan.FromSeconds(5)));
                        var exceptionInnerExceptions = subscriptions[i - 1].SubscriptionLifetimeTask.Exception.InnerException;
                        Assert.NotNull(exceptionInnerExceptions);
                        Assert.True(exceptionInnerExceptions is SubscriptionInUseException);
                    }
                }
            }
        }

        [Fact]
        public void ShouldReplaceActiveClientWhen_ForceAndKeep_StrategyIsUsed()
        {
            using (var store = GetDocumentStore())
            {
                foreach (var strategyToReplace in new[] { SubscriptionOpeningStrategy.OpenIfFree, SubscriptionOpeningStrategy.TakeOver, SubscriptionOpeningStrategy.WaitForFree })
                {
                    var id = store.Subscriptions.Create(new SubscriptionCriteria<User>());
                    var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(id)
                    {
                        Strategy = strategyToReplace
                    });
                    
                    var items = new BlockingCollection<User>();

                    subscription.Subscribe(items.Add);

                    subscription.Start();
                    var forcedSubscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(id)
                    {
                        Strategy = SubscriptionOpeningStrategy.ForceAndKeep
                    });

                    var forcedItems = new BlockingCollection<User>();

                    forcedSubscription.Subscribe(forcedItems.Add);


                    forcedSubscription.Start();

                    using (var s = store.OpenSession())
                    {
                        s.Store(new User());
                        s.Store(new User());

                        s.SaveChanges();
                    }

                    User user;

                    Assert.True(forcedItems.TryTake(out user, waitForDocTimeout));
                    Assert.True(forcedItems.TryTake(out user, waitForDocTimeout));

                    Assert.True(SpinWait.SpinUntil(() => subscription.IsConnectionClosed, TimeSpan.FromSeconds(5)));

                    var exceptionInnerExceptions = subscription.SubscriptionLifetimeTask.Exception.InnerException;
                    Assert.NotNull(exceptionInnerExceptions);
                    Assert.True(exceptionInnerExceptions is SubscriptionInUseException);
                }
            }
        }

        [Fact]
        public void OpenIfFree_And_TakeOver_StrategiesCannotDropClientWith_ForceAndKeep_Strategy()
        {
            using (var store = GetDocumentStore())
            {
                var id = store.Subscriptions.Create(new SubscriptionCriteria<User>());

                var forcedSubscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(id)
                {
                    Strategy = SubscriptionOpeningStrategy.ForceAndKeep
                });

                forcedSubscription.Subscribe(x => { });
                forcedSubscription.Start();

                foreach (var strategy in new[] { SubscriptionOpeningStrategy.OpenIfFree, SubscriptionOpeningStrategy.TakeOver })
                {
                    var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(id)
                    {
                        Strategy = strategy
                    });
                    subscription.Subscribe(x => { });
                    
                    Assert.Throws<SubscriptionInUseException>(() =>
                    {
                        subscription.Start();
                    });
                }
            }
        }

        [Fact]
        
        public void ForceAndKeep_StrategyUsageCanTakeOverAnotherClientWith_ForceAndKeep_Strategy()
        {
            using (var store = GetDocumentStore())
            {
                var id = store.Subscriptions.Create(new SubscriptionCriteria<User>());

                const int numberOfClients = 4;

                var subscriptions = new Subscription<User>[numberOfClients];
                try
                {
                    var items = new BlockingCollection<User>[numberOfClients];

                    for (int i = 0; i < numberOfClients; i++)
                    {
                        subscriptions[i] = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(id)
                        {
                            Strategy = SubscriptionOpeningStrategy.ForceAndKeep
                        });

                        items[i] = new BlockingCollection<User>();

                        subscriptions[i].Subscribe(items[i].Add);

                        subscriptions[i].Start();

                        bool batchAcknowledged = false;

                        subscriptions[i].AfterAcknowledgment += () => batchAcknowledged = true;

                        using (var s = store.OpenSession())
                        {
                            s.Store(new User());
                            s.Store(new User());

                            s.SaveChanges();
                        }

                        User user;

                        Assert.True(items[i].TryTake(out user, waitForDocTimeout), $"Waited for {waitForDocTimeout.TotalSeconds} seconds to get notified about a user, giving up... ");
                        Assert.True(items[i].TryTake(out user, waitForDocTimeout), $"Waited for {waitForDocTimeout.TotalSeconds} seconds to get notified about a user, giving up... ");

                        SpinWait.SpinUntil(() => batchAcknowledged, TimeSpan.FromSeconds(5)); // let it acknowledge the processed batch before we open another subscription
                        Assert.True(batchAcknowledged, "Wait for 5 seconds for batch to be acknoeledged, giving up...");
                        if (i > 0)
                        {
                            Assert.False(items[i - 1].TryTake(out user, TimeSpan.FromSeconds(2)),
                                "Was able to take a connection to subscription even though a new connection was opened with ForceAndKeep strategy.");
                            Assert.True(SpinWait.SpinUntil(() => subscriptions[i - 1].IsConnectionClosed, TimeSpan.FromSeconds(5)),
                                "Previous connection to subscription was not closed even though a new connection was opened with ForceAndKeep strategy.");

                            var exceptionInnerExceptions = subscriptions[i - 1].SubscriptionLifetimeTask.Exception.InnerException;
                            Assert.NotNull(exceptionInnerExceptions);
                            Assert.True(exceptionInnerExceptions is SubscriptionInUseException, "SubscriptionConnectionException is not set to expected type, SubscriptionInUseException.");
                        }
                    }
                }
                finally
                {
                    foreach (var subscription in subscriptions)
                    {
                        subscription?.Dispose();
                    }
                }
            }
        }

        [Fact]
        
        public void ShouldOpenSubscriptionWith_WaitForFree_StrategyWhenItIsNotInUseByAnotherClient()
        {
            using (var store = GetDocumentStore())
            {
                var id = store.Subscriptions.Create(new SubscriptionCriteria<User>());
                var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(id)
                {
                    Strategy = SubscriptionOpeningStrategy.WaitForFree
                });

                var items = new BlockingCollection<User>();

                subscription.Subscribe(items.Add);
                subscription.Start();

                using (var s = store.OpenSession())
                {
                    s.Store(new User());
                    s.Store(new User());

                    s.SaveChanges();
                }

                User user;

                Assert.True(items.TryTake(out user, waitForDocTimeout));
                Assert.True(items.TryTake(out user, waitForDocTimeout));
            }
        }

        [Fact]
        
        public void ShouldProcessSubscriptionAfterItGetsReleasedWhen_WaitForFree_StrategyIsSet()
        {
            using (var store = GetDocumentStore())
            {
                var id = store.Subscriptions.Create(new SubscriptionCriteria<User>());

                var userId = 0;

                foreach (var activeClientStrategy in new[] { SubscriptionOpeningStrategy.OpenIfFree, SubscriptionOpeningStrategy.TakeOver, SubscriptionOpeningStrategy.ForceAndKeep })
                {
                    var active = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(id)
                    {
                        Strategy = activeClientStrategy
                    });

                    var pending = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(id)
                    {
                        Strategy = SubscriptionOpeningStrategy.WaitForFree
                    });


                    Assert.Null(pending.SubscriptionLifetimeTask.Exception);
                    Assert.False(pending.IsConnectionClosed);

                    bool batchAcknowledged = false;
                    pending.AfterAcknowledgment += () => batchAcknowledged = true;

                    var items = new BlockingCollection<User>();

                    pending.Subscribe(items.Add);
                    pending.Start();

                    active.Dispose(); // disconnect the active client, the pending one should be notified the the subscription is free and retry to open it

                    using (var s = store.OpenSession())
                    {
                        s.Store(new User(), "users/" + userId++);
                        s.Store(new User(), "users/" + userId++);

                        s.SaveChanges();
                    }

                    User user;

                    Assert.True(items.TryTake(out user, waitForDocTimeout));
                    Assert.Equal("users/" + (userId - 2), user.Id);
                    Assert.True(items.TryTake(out user, waitForDocTimeout));
                    Assert.Equal("users/" + (userId - 1), user.Id);

                    Assert.True(SpinWait.SpinUntil(() => batchAcknowledged, TimeSpan.FromSeconds(5))); // let it acknowledge the processed batch before we open another subscription

                    pending.Dispose();
                }
            }
        }

        [Fact]
        
        public void AllClientsWith_WaitForFree_StrategyShouldGetAccessToSubscription()
        {
            using (var store = GetDocumentStore())
            {
                var id = store.Subscriptions.Create(new SubscriptionCriteria<User>());

                const int numberOfClients = 4;

                var subscriptions = new Subscription<User>[numberOfClients];
                var processed = new bool[numberOfClients];

                int? processedClient = null;

                for (int i = 0; i < numberOfClients; i++)
                {
                    var clientNumber = i;

                    subscriptions[clientNumber] = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(id)
                    {
                        Strategy = SubscriptionOpeningStrategy.WaitForFree
                    });

                    subscriptions[clientNumber].AfterBatch += x =>
                    {
                        processed[clientNumber] = true;
                    };

                    subscriptions[clientNumber].Subscribe(x =>
                    {
                        processedClient = clientNumber;
                    });

                    subscriptions[clientNumber].StartAsync().Wait(100);
                }

                for (int i = 0; i < numberOfClients; i++)
                {
                    using (var s = store.OpenSession())
                    {
                        s.Store(new User());
                        s.SaveChanges();
                    }

                    Assert.True(SpinWait.SpinUntil(() => processedClient != null, waitForDocTimeout));
                    Assert.True(SpinWait.SpinUntil(() => processed[processedClient.Value], waitForDocTimeout));

                    subscriptions[processedClient.Value].Dispose();

                    processedClient = null;
                }

                for (int i = 0; i < numberOfClients; i++)
                {
                    Assert.True(processed[i]);
                }
            }
        }
    }
}
