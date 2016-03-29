using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NLog;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Voron;
using Voron.Data.Tables;
using System.Threading;
using System.Collections.Concurrent;
using Raven.Database.Util;
using System.Diagnostics;
using Microsoft.AspNet.Server.Kestrel.Infrastructure;
using Raven.Server.Json.Parsing;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Server.ServerWide;
using System.IO;

namespace Raven.Server.Documents
{
    public class SubscriptionStorage:IDisposable
    {
        private readonly TransactionContextPool _contextPool;
        private Logger _log; //todo: add logging
        private readonly TableSchema _subscriptionsSchema = new TableSchema();

        private readonly ConcurrentDictionary<long, object> _locks = new ConcurrentDictionary<long, object>();
        private readonly ConcurrentDictionary<long, SizeLimitedConcurrentSet<string>> _forciblyReleasedSubscriptions = new ConcurrentDictionary<long, SizeLimitedConcurrentSet<string>>();

        public static TimeSpan TwoMinutesTimespan = TimeSpan.FromMinutes(2);

        private readonly ConcurrentDictionary<long, BlittableJsonReaderObject> _openSubscriptions =
            new ConcurrentDictionary<long, BlittableJsonReaderObject>();
        private readonly MemoryOperationContext _subscriptionsContext;

        public static List<string> _criteriaFields = new List<string>
        {
            "KeyStartsWith",
            "StartEtag",
            "BelongsToAnyCollection",
            "PropertiesMatch",
            "PropertiesNotMatch"
        };

        private UnmanagedBuffersPool _unmanagedBuffersPool;
        private StorageEnvironment _environment;
        private DocumentDatabase _db;

        public SubscriptionStorage(DocumentDatabase db)
        {
            _db = db;
            var options = _db.Configuration.Core.RunInMemory
               ? StorageEnvironmentOptions.CreateMemoryOnly()
               : StorageEnvironmentOptions.ForPath(Path.Combine(_db.Configuration.Core.DataDirectory,"Subscriptions"));

            _environment = new StorageEnvironment(options);
            _unmanagedBuffersPool = new UnmanagedBuffersPool($"Subscriptions");
            _contextPool = new TransactionContextPool(_unmanagedBuffersPool, _environment);

            _contextPool.AllocateOperationContext(out _subscriptionsContext);
            var databaseName = db.Name;
            _log = LogManager.GetLogger($"{typeof(SubscriptionStorage).FullName}.{databaseName}");
            _subscriptionsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1,
                IsGlobal = true,
                Name = "Subscriptions"
            });            
        }

        public void Initialize()
        {
            TransactionOperationContext context;
            using (var itx = _environment.WriteTransaction())
            {                
                itx.CreateTree("Subscriptions");
                _subscriptionsSchema.Create(itx, "SubscriptionsSchema");
                itx.Commit();
            }
        }        

        public unsafe long CreateSubscription(BlittableJsonReaderObject criteria)
        {
            using (var tx = _environment.WriteTransaction())
            {
                var table = new Table(_subscriptionsSchema, "Subscriptions", tx);
                var subscriptionsTree = tx.ReadTree(Schema.SubscriptionsTreee);
                long id = subscriptionsTree.Increment(Schema.Id, 1);

                long timeOfSendingLastBatch = 0;
                long timeOfLastClientActivity = 0;
                long ackEtag = 0;

                if (criteria.Count > _criteriaFields.Count)
                    throw new ArgumentException("Subscription Criteria Is In Illegal Format");

                for (var i = 0; i < criteria.Count; i++)
                {
                    var propertyTuple = criteria.GetPropertyByIndex(i);

                    bool fieldIdentified = false;
                    // ReSharper disable once ForCanBeConvertedToForeach
                    // ReSharper disable once LoopCanBeConvertedToQuery
                    for (var j = 0; j < _criteriaFields.Count; j++)
                    {
                        if (propertyTuple.Item1.CompareTo(_criteriaFields[j]) != 0) continue;
                        fieldIdentified = true;
                        break;
                    }
                    if (fieldIdentified != false) continue;
                    if (criteria.Count > _criteriaFields.Count)
                        throw new ArgumentException($"Subscription Criteria Is In Illegal Format, the field {propertyTuple.Item1.ToString()} should not be there");
                }
                var tvb = new TableValueBuilder
                {
                    {(byte*)&id,sizeof(long) },
                    {criteria.BasePointer,criteria.Size},
                    {(byte*)&ackEtag,sizeof(long) },
                    {(byte*)&timeOfSendingLastBatch, sizeof(long) },
                    {(byte*)&timeOfLastClientActivity, sizeof(long) }
                };

                table.Insert(tvb);
                tx.Commit();
                return id;
            }
            
        }

        public unsafe void UpdateClientActivityDate(long id, Voron.Impl.Transaction exTx = null)
        {
            var subscriptionLocker = _locks.GetOrAdd(id, LockerValueFactory);
            
            try
            {                
                if (Monitor.TryEnter(subscriptionLocker, TwoMinutesTimespan) == false)
                {
                    throw new SynchronizationLockException($"Could not get serial lock for subscription {id}, Aborting operation");
                }

                var tx = exTx ?? _environment.WriteTransaction();
                try
                {
                    var table = new Table(_subscriptionsSchema, "Subscriptions", tx);
                    var subscriptionId = id;
                    var oldValue = table.ReadByKey(new Slice((byte*)&subscriptionId, sizeof(long)));

                    if (oldValue == null)
                        throw new ArgumentException($"Cannot update subscription with id {id}, because it was not found");

                    int oldCriteriaSize;
                    int longSizeForOutput;

                    var ticks = SystemTime.UtcNow.Ticks;
                    var tvb = new TableValueBuilder
                        {
                            {(byte*)&subscriptionId,sizeof(long) },
                            {oldValue.Read(Schema.SubscriptionTable.CriteriaIndex, out oldCriteriaSize),oldCriteriaSize},
                            {oldValue.Read(Schema.SubscriptionTable.AckEtagIndex, out longSizeForOutput),sizeof(long) },
                            {oldValue.Read(Schema.SubscriptionTable.TimeOfSendingLastBatch, out longSizeForOutput), sizeof(long) },
                            {(byte*)& ticks, sizeof(long) }
                        };
                    table.Update(subscriptionId, tvb);
                }
                finally
                {
                    if (exTx == null)
                    {
                        tx.Commit();
                        tx.Dispose();
                    }
                }
            }
            finally
            {                     
                Monitor.Exit(subscriptionLocker);
            }           
        }

        public unsafe void UpdateBatchSentTime(long id)
        {
            var subscriptionLocker = _locks.GetOrAdd(id, LockerValueFactory);
            try
            {
                if (Monitor.TryEnter(subscriptionLocker, TwoMinutesTimespan) == false)
                {
                    throw new SynchronizationLockException($"Could not get serial lock for subscription {id}, Aborting operation");
                }

                using (var tx = _environment.WriteTransaction())
                {
                    var table = new Table(_subscriptionsSchema, "Subscriptions", tx);
                    var subscriptionId = id;
                    var oldValue = table.ReadByKey(new Slice((byte*)&subscriptionId, sizeof(long)));

                    if (oldValue == null)
                        throw new ArgumentException($"Cannot update subscription with id {id}, because it was not found");

                    int oldCriteriaSize;
                    int longSizeForOutput;
                    var ticks = SystemTime.UtcNow.Ticks;
                    var tvb = new TableValueBuilder
                            {
                                {(byte*)&subscriptionId,sizeof(long) },
                                {oldValue.Read(Schema.SubscriptionTable.CriteriaIndex, out oldCriteriaSize),oldCriteriaSize},
                                {oldValue.Read(Schema.SubscriptionTable.AckEtagIndex, out longSizeForOutput),sizeof(long) },
                                {(byte*)& ticks, sizeof(long) },
                                {oldValue.Read(Schema.SubscriptionTable.TimeOfLastActivityIndex, out longSizeForOutput), sizeof(long) }
                            };
                    table.Update(subscriptionId, tvb);
                    tx.Commit();
                }
                
            }
            finally
            {
                Monitor.Exit(subscriptionLocker);
            }
        }

        //todo: remove that funciton
        private long GetTicksFromField(BlittableJsonReaderObject reader, string fieldName)
        {
            // First we'll try getting the "long" value, then, if we fail, we'll try to get the string value and parse it to TimeSpan
            // We better change the way we represent timespans to longs. For now, TimeSpan.ToString() default funtionality has 
            // the format: "dddddd.hh:mm:ss.ttttttt" (t-ticks)
            try
            {
                long result;
                reader.TryGet(fieldName, out result);
                return result;
            }
            catch(FormatException ex)
            {
                string timeSpanString;
                reader.TryGet<string>(fieldName, out timeSpanString);
                return TimeSpan.Parse(timeSpanString).Ticks;
            }
        }

        private void ForceReleaseAndOpenForNewClient(long id, BlittableJsonReaderObject options, Voron.Impl.Transaction tx)
        {
            ReleaseSubscription(id);
            _openSubscriptions.TryAdd(id, options);
            UpdateClientActivityDate(id, tx);
        }

        public unsafe void OpenSubscription(long id, BlittableJsonReaderObject subscriptionConnectionOptions)
        {
            SizeLimitedConcurrentSet<string> releasedConnections;
            string connectionId;
            subscriptionConnectionOptions.TryGet<string>("ConnectionId", out connectionId);

            if (_forciblyReleasedSubscriptions.TryGetValue(id, out releasedConnections) && releasedConnections.Contains(connectionId))
                throw new SubscriptionClosedException("Subscription " + id + " was forcibly released. Cannot reopen it.");

            
            // if subscription is not opened, store subscription connection options and update subscription activity
            if (_openSubscriptions.ContainsKey(id) == false)
            {
                var optionsMemoryCopy = _subscriptionsContext.GetMemory(subscriptionConnectionOptions.Size);
                subscriptionConnectionOptions.CopyTo((byte*)optionsMemoryCopy.Address);
                var optionsCopy = new BlittableJsonReaderObject((byte*)optionsMemoryCopy.Address, subscriptionConnectionOptions.Size, _subscriptionsContext);

                if (_openSubscriptions.TryAdd(id, optionsCopy))
                {
                    UpdateClientActivityDate(id);
                    return;
                }
            }

            // check if there is already opened subscription connection with the same id
            BlittableJsonReaderObject existingOptions;

            if (_openSubscriptions.TryGetValue(id, out existingOptions) == false)
                throw new SubscriptionDoesNotExistException("Didn't get existing open subscription while it's expected. Subscription id: " + id);
            string existingOptionsConnectionId;
            existingOptions.TryGet<string>("ConnectionId", out existingOptionsConnectionId);

            if (existingOptionsConnectionId.Equals(connectionId, StringComparison.OrdinalIgnoreCase))
            {
                // reopen subscription on already existing connection - might happen after network connection problems the client tries to reopen
                UpdateClientActivityDate(id);
                return;
            }
            using (var tx = _environment.WriteTransaction())
            {
                var table = new Table(_subscriptionsSchema, "Subscriptions", tx);
                var subscriptionId = id;
                var config = table.ReadByKey(new Slice((byte*)&subscriptionId, sizeof(long)));
                var now = SystemTime.UtcNow.Ticks;
                int readSize;
                var timeSinceBatchSentTicks = now - *(long*)config.Read(Schema.SubscriptionTable.TimeOfSendingLastBatch, out readSize);

                BlittableJsonReaderObject batchOptionsReader;

                existingOptions.TryGet("BatchOptions", out batchOptionsReader);

                // todo: consider not to use DateTime at all, and use ticks(long) all the way, 
                // will require creating a new subscription document, based on the one we receive from the client
                var acknowledgementTimeoutTicks = GetTicksFromField(batchOptionsReader, "AcknowledgmentTimeout");
                int tempSize;
                var timeOfLastClientActivityTicks = *(long*)config.Read(Schema.SubscriptionTable.TimeOfLastActivityIndex, out tempSize);

                var clientAliveNotificationTicks = GetTicksFromField(existingOptions, "ClientAliveNotificationInterval");

                if (timeSinceBatchSentTicks > acknowledgementTimeoutTicks &&
                now - timeOfLastClientActivityTicks > clientAliveNotificationTicks * 3)
                {
                    // last connected client exceeded ACK timeout and didn't send at least two 'client-alive' notifications - let the requesting client to open it
                    ForceReleaseAndOpenForNewClient(id, subscriptionConnectionOptions, tx);
                    tx.Commit();
                    return;
                }

                SubscriptionOpeningStrategy subscriptionConnectionStrategy;

                subscriptionConnectionOptions.TryGet("Strategy", out subscriptionConnectionStrategy);
                switch (subscriptionConnectionStrategy)
                {
                    case SubscriptionOpeningStrategy.TakeOver:
                        SubscriptionOpeningStrategy existingOptionsConnectionStrategy;
                        existingOptions.TryGet("Strategy", out existingOptionsConnectionStrategy);
                        if (existingOptionsConnectionStrategy != SubscriptionOpeningStrategy.ForceAndKeep)
                        {
                            ForceReleaseAndOpenForNewClient(id, subscriptionConnectionOptions, tx);
                            tx.Commit();
                            return;
                        }
                        break;
                    case SubscriptionOpeningStrategy.ForceAndKeep:
                        ForceReleaseAndOpenForNewClient(id, subscriptionConnectionOptions, tx);
                        tx.Commit();
                        return;
                }
                throw new SubscriptionInUseException("Subscription is already in use. There can be only a single open subscription connection per subscription.");
            }
            
        }

        public void ReleaseSubscription(long id, bool forced = false)
        {
            BlittableJsonReaderObject options;
            _openSubscriptions.TryRemove(id, out options);

            string connectionId;
            options.TryGet("ConnectionId", out connectionId);

            if (forced && options != null)
            {
                _forciblyReleasedSubscriptions.GetOrAdd(id, new SizeLimitedConcurrentSet<string>(50, StringComparer.OrdinalIgnoreCase)).Add(connectionId);
            }
        }


        public unsafe void AcknowledgeBatchProcessed(long id, long lastEtag)
        {
            using (var tx = _environment.WriteTransaction())
            {
                var config = GetSubscriptionConfig(id,tx);
                var options = GetBatchOptions(id);
                var acknoledgementTimeout = GetTicksFromField(options, "AcknowledgmentTimeout");

                var tempSize = 0;
                var timeSinceBatchSent = SystemTime.UtcNow.Ticks - *(long*)config.Read(Schema.SubscriptionTable.TimeOfSendingLastBatch, out tempSize); //config.TimeOfSendingLastBatch;
                if (timeSinceBatchSent > acknoledgementTimeout)
                    throw new TimeoutException("The subscription cannot be acknowledged because the timeout has been reached.");

                var subscriptionId = id;

                int oldCriteriaSize;
                int longSizeForOutput;
                long now = SystemTime.UtcNow.Ticks;
                var tvb = new TableValueBuilder
                            {
                                {(byte*)&subscriptionId,sizeof(long) },
                                {config.Read(Schema.SubscriptionTable.CriteriaIndex, out oldCriteriaSize),oldCriteriaSize},
                                {(byte*)&lastEtag,sizeof(long) },
                                {config.Read(Schema.SubscriptionTable.TimeOfSendingLastBatch, out longSizeForOutput), sizeof(long) },
                                {(byte*)&now, sizeof(long) }
                            };
                var table = new Table(_subscriptionsSchema, "Subscriptions",tx);
                table.Update(subscriptionId, tvb);
                tx.Commit();
            }
        }

        public void AssertOpenSubscriptionConnection(long id, string connection)
        {
            BlittableJsonReaderObject options;
            if (_openSubscriptions.TryGetValue(id, out options) == false)
                throw new SubscriptionClosedException("There is no subscription with id: " + id + " being opened");
            string connectionIdInOptions;
            if (options.TryGet("ConnectionId", out connectionIdInOptions) == false)
            {
                throw new SubscriptionInUseException("Unexpected Error: Subscription ConnectionId not found in connnection options.");
            }
            if (connectionIdInOptions.Equals(connection, StringComparison.OrdinalIgnoreCase) == false)
            {
                // prevent from concurrent work of multiple clients against the same subscription
                throw new SubscriptionInUseException("Subscription is being opened for a different connection.");
            }
        }

        public BlittableJsonReaderObject GetBatchOptions(long id)
        {
            BlittableJsonReaderObject options;
            if (_openSubscriptions.TryGetValue(id, out options) == false)
                throw new SubscriptionClosedException("There is no open subscription with id: " + id);

            return options;
        }

        public unsafe TableValueReader GetSubscriptionConfig(long id, Voron.Impl.Transaction tx = null)
        {
            var localTx = tx??_environment.ReadTransaction();
            try
            {
                var table = new Table(_subscriptionsSchema, "Subscriptions", localTx);
                var subscriptionId = id;

                var config = table.ReadByKey(new Slice((byte*)&subscriptionId, sizeof(long)));

                if (config == null)
                    throw new SubscriptionDoesNotExistException("There is no subscription configuration for specified identifier (id: " + id + ")");
                return config;
            }
            finally
            {
                if (tx == null)
                {
                    localTx.Commit();
                    localTx.Dispose();
                }
            }            
        }

        public unsafe void AssertSubscriptionConfigExists(long id)
        {
            using (var tx = _environment.WriteTransaction())
            {
                var table = new Table(_subscriptionsSchema, "Subscriptions", tx);
                var subscriptionId = id;

                if (table.VerifyKeyExists(new Slice((byte*)&subscriptionId, sizeof(long))) == false)
                    throw new SubscriptionDoesNotExistException("There is no subscription configuration for specified identifier (id: " + id + ")");
                tx.Commit();
            }
        }

        public void DeleteSubscription(long id)
        {
            var subscriptionLocker = _locks.GetOrAdd(id, LockerValueFactory);
            try
            {
                if (Monitor.TryEnter(subscriptionLocker, TwoMinutesTimespan) == false)
                {
                    throw new SynchronizationLockException($"Could not get serial lock for subscription {id}, Aborting operation");
                }
                using (var tx = _environment.WriteTransaction())
                {
                    var table = new Table(_subscriptionsSchema, "Subscriptions", tx);
                    table.Delete(id);

                    SizeLimitedConcurrentSet<string> temp;
                    _forciblyReleasedSubscriptions.TryRemove(id, out temp);
                    tx.Commit();
                }
            }
            finally
            {
                Monitor.Exit(subscriptionLocker);
            }
        }

        public List<TableValueReader> GetSubscriptions(int start, int take)
        {
            var subscriptions = new List<TableValueReader>();

            using (var tx = _environment.ReadTransaction())
            {
                var table = new Table(_subscriptionsSchema, "Subscriptions", tx);
                var seen = 0;
                var taken = 0;

                foreach (var subscriptionForKey in table.SeekByPrimaryKey(Slice.BeforeAllKeys))
                {                    
                    if (seen < start)
                    {
                        seen++;
                        continue;
                    }

                    subscriptions.Add(subscriptionForKey);

                    if (taken > take)
                        break;
                    
                }
                
                return subscriptions;
            }
        }

        private object LockerValueFactory(long id)
        {
            return new object();
        }

        public void Dispose()
        {
            _subscriptionsContext.Dispose();
        }

        public unsafe List<Tuple<TableValueReader, BlittableJsonReaderObject>> GetDebugInfo()
        {
            using (var tx = _environment.ReadTransaction())
            {
                var subscriptions = new List<Tuple<TableValueReader, BlittableJsonReaderObject>>();

                var table = new Table(_subscriptionsSchema, "Subscriptions", tx);

                foreach (var subscriptionsForKey in table.SeekForwardFrom(_subscriptionsSchema.Key, Slice.BeforeAllKeys))
                {
                    foreach (var subscriptionForKey in subscriptionsForKey.Results)
                    {
                        int longSize;
                        var subscriptionId = *(long*)subscriptionForKey.Read(0, out longSize);
                        BlittableJsonReaderObject options = null;
                        _openSubscriptions.TryGetValue(subscriptionId, out options);
                        subscriptions.Add(Tuple.Create(subscriptionForKey, options));
                    }
                }

                return subscriptions;
            }
        }

        // ReSharper disable once ClassNeverInstantiated.Local
        public class Schema
        {
            public static readonly string SubscriptionsTreee = "SubscriptionsIDs";
            public static readonly Slice Id = "Id";

            public static class SubscriptionTable
            {
#pragma warning disable 169
                public static readonly int IdIndex = 0;
                public static readonly int CriteriaIndex = 1;
                public static readonly int AckEtagIndex = 2;
                public static readonly int TimeOfSendingLastBatch = 3;
                public static readonly int TimeOfLastActivityIndex = 4;
#pragma warning restore 169
            }

        }
    }
}
