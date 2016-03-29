using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Abstractions.Logging;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Handlers
{
    public class SubscriptionsHandler : DatabaseRequestHandler
    {
        private static readonly ILog log = LogManager.GetLogger(typeof (SubscriptionsHandler));
        
        [RavenAction("/databases/*/subscription/create", "GET", "/databases/{databaseName:string}/subscription/create")]
        public async Task Create()
        {
            DocumentsOperationContext context;
            long subscriptionId = 0;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var subscriptionCriteriaRaw = await context.ReadForDiskAsync(RequestBodyStream(), null);
                context.OpenWriteTransaction();
                subscriptionId = Database.SubscriptionStorage.CreateSubscription(subscriptionCriteriaRaw);
                context.Transaction.Commit();
                var ack = new DynamicJsonValue
                {
                    ["Id"] = subscriptionId
                };
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, ack);
                }
                
                HttpContext.Response.StatusCode = 201; // NoContent
                
            }
        }

        [RavenAction("/databases/*/subscription", "DELETE",
            "/databases/{databaseName:string}/subscription?id={subscriptionId:long}")]
        public Task Delete()
        {
            var ids = HttpContext.Request.Query["id"];
            if (ids.Count == 0)
                throw new ArgumentException("The 'id' query string parameter is mandatory");

            long id;
            if (long.TryParse(ids[0], out id) == false)
                throw new ArgumentException("The 'id' query string parameter must be a valid long");

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                context.OpenWriteTransaction();
                Database.SubscriptionStorage.DeleteSubscription(id);
                context.Transaction.Commit();
            }

            HttpContext.Response.StatusCode = 204; // NoContent

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/subscription/open", "POST",
            "/databases/{databaseName:string}/subscription/open?id={subscriptionId:long}")]
        public Task Open()
        {
            var id = GetLongQueryString("id");

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                Database.SubscriptionStorage.AssertSubscriptionConfigExists(id);
                var options = context.ReadForDisk(HttpContext.Request.Body, "Subscriptions");
                Database.SubscriptionStorage.OpenSubscription(id, options);
            }

            Database.Notifications.RaiseNotifications(new DataSubscriptionChangeNotification
            {
                Id = id,
                Type = DataSubscriptionChangeTypes.SubscriptionOpened
            });

            return Task.CompletedTask;
        }

        /*[HttpGet]
        [RavenRoute("subscriptions/pull")]
        [RavenRoute("/databases/{databaseName}/subscriptions/pull")]
        public HttpResponseMessage Pull(long id, string connection)
        {
            Database.Subscriptions.AssertOpenSubscriptionConnection(id, connection);

            var pushStreamContent = new PushStreamContent((stream, content, transportContext) => StreamToClient(id, Database.Subscriptions, stream))
            {
                Headers =
                {
                    ContentType = new MediaTypeHeaderValue("application/json")
                    {
                        CharSet = "utf-8"
                    }
                }
            };

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = pushStreamContent
            };
        }*/

        [RavenAction("/databases/*/subscription/acknowledgeBatch", "POST",
            "/databases/{databaseName:string}/subscription/acknowledgeBatch?id={subscriptionId:string}&lastEtag={lastEtag:long}&connection={connection:string}"
            )]
        public Task AcknowledgeBatch()
        {
            var id = GetLongQueryString("id");
            var lasEtag = GetLongQueryString("lastEtag");
            var connection = GetStringQueryString("connection");

            Database.SubscriptionStorage.AssertOpenSubscriptionConnection(id, connection);

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                try
                {
                    context.OpenWriteTransaction();
                    Database.SubscriptionStorage.AcknowledgeBatchProcessed(id, lasEtag);
                    context.Transaction.Commit();
                }
                catch (TimeoutException)
                {
                    var responseStream = ResponseBodyStream();
                    var buffer = context.GetManagedBuffer();
                    var message = "The subscription cannot be acknowledged because the timeout has been reached.";
                    var byteLength = CopyStringToBuffer(buffer, message);
                    responseStream.Write(buffer, 0, byteLength);
                    HttpContext.Response.StatusCode = 408; // RequestTimout
                }
            }

            return Task.CompletedTask;
        }

        private unsafe int CopyStringToBuffer(byte[] buffer, string message)
        {
            var messageByteLength = Encoding.UTF8.GetByteCount(message);

            if (messageByteLength > buffer.Length)
                // ReSharper disable once NotResolvedInText
                throw new ArgumentOutOfRangeException("Message cannot be bigger than buffer");
            fixed (byte* bufferPtr = buffer)
            fixed (char* chars = message)
            {
                Encoding.UTF8.GetBytes(chars, message.Length, bufferPtr, messageByteLength);
            }

            return messageByteLength;
        }
                
        [RavenAction("/databases/*/subscription/close", "POST",
            "/databases/{databaseName:string}/subscription/close?id={subscriptionId:string}&connection={connection:string}&force={force:long}")]
        public Task Close()
        {
            var id = GetLongQueryString("id");
            var force = GetBoolValueQueryString("force");
            var connection = GetStringQueryString("connection");

            if (force == false)
            {
                try
                {
                    Database.SubscriptionStorage.AssertOpenSubscriptionConnection(id, connection);
                }
                catch (SubscriptionException)
                {
                    // ignore if assertion exception happened on close
                    HttpContext.Response.StatusCode = 200; 
                    return Task.CompletedTask;
                }
            }
            HttpContext.Response.StatusCode = 200; 
            Database.SubscriptionStorage.ReleaseSubscription(id, force);

            Database.Notifications.RaiseNotifications(new DataSubscriptionChangeNotification
            {
                Id = id,
                Type = DataSubscriptionChangeTypes.SubscriptionReleased
            });

            return Task.CompletedTask;
        }
        
        [RavenAction("/databases/*/subscription/client-alive", "PATCH", 
            "/databases/{databaseName:string}/subscription/client-alive?id={subscriptionId:string}&connection={connection:string}")]
        public Task ClientAlive()
        {
            var id = GetLongQueryString("id");
            var connection = GetStringQueryString("connection");

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                Database.SubscriptionStorage.AssertOpenSubscriptionConnection(id, connection);
                context.OpenWriteTransaction();
                Database.SubscriptionStorage.UpdateClientActivityDate(id);
                context.Transaction.Commit();
            }
            HttpContext.Response.StatusCode = 200;
            return Task.CompletedTask;
        }


       

        [RavenAction("/databases/*/subscription", "GET", "/databases/{databaseName:string}/subscription?start={start:int}&pageSize={pageSize:int}")]
        public Task Get()
        {
            var start = GetStart();
            var take = GetPageSize(Database.Configuration.Core.MaxPageSize);
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();
                var subscriptionTableValues = Database.SubscriptionStorage.GetSubscriptions(start, take);
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    WriteSubscriptionTableValues(writer, context, subscriptionTableValues);
                }
            }
            HttpContext.Response.StatusCode = 200;
            return Task.CompletedTask;
        }

        /* public class SubscriptionConfig
           {
               public long SubscriptionId { get; set; }
               public SubscriptionCriteria Criteria { get; set; }
               public long? AckEtag { get; set; }
               public long TimeOfSendingLastBatch { get; set; }
               public long TimeOfLastClientActivity { get; set; }
           }*/

        private unsafe void WriteSubscriptionTableValues(BlittableJsonTextWriter writer,
            DocumentsOperationContext context, IEnumerable<TableValueReader> subscriptions)
        {
            writer.WriteStartArray();
            foreach (var subscriptionTableValue in subscriptions)
            {
                writer.WriteStartObject();
                int longSize;
                var subscriptionId =
                    *(long*)subscriptionTableValue.Read(SubscriptionStorage.Schema.SubscriptionTable.IdIndex, out longSize);
                int criteriaSize;
                var criteriaPtr =
                    subscriptionTableValue.Read(SubscriptionStorage.Schema.SubscriptionTable.CriteriaIndex, out criteriaSize);
                var ackEtag =
                    *(long*)subscriptionTableValue.Read(SubscriptionStorage.Schema.SubscriptionTable.AckEtagIndex, out longSize);
                var timeOfSendingLastBatch =
                    *(long*)subscriptionTableValue.Read(SubscriptionStorage.Schema.SubscriptionTable.TimeOfSendingLastBatch, out longSize);
                var timeOfLastClientActivity =
                    *(long*)subscriptionTableValue.Read(SubscriptionStorage.Schema.SubscriptionTable.TimeOfLastActivityIndex, out longSize);

                writer.WritePropertyName(context.GetLazyStringForFieldWithCaching("Id"));
                writer.WriteInteger(subscriptionId);
                writer.WritePropertyName(context.GetLazyStringForFieldWithCaching("Criteria"));
                context.Write(writer, new BlittableJsonReaderObject(criteriaPtr, criteriaSize, context));
                writer.WritePropertyName(context.GetLazyStringForFieldWithCaching("AckEtag"));
                writer.WriteInteger(ackEtag);
                writer.WritePropertyName(context.GetLazyStringForFieldWithCaching("TimeOfSendingLastBatch"));
                writer.WriteInteger(timeOfSendingLastBatch);
                writer.WritePropertyName(context.GetLazyStringForFieldWithCaching("TimeOfLastClientActivity"));
                writer.WriteInteger(timeOfLastClientActivity);
                writer.WriteEndObject();
            }
            writer.WriteEndArray();
        }

        /*private void StreamToClient(long id, SubscriptionActions subscriptions, Stream stream)
        {
            var sentDocuments = false;

            var bufferStream = new BufferedStream(stream, 1024 * 64);
            using (var writer = new JsonTextWriter(new StreamWriter(bufferStream)))
            {
                var options = subscriptions.GetBatchOptions(id);

                writer.WriteStartObject();
                writer.WritePropertyName("Results");
                writer.WriteStartArray();

                using (var cts = new CancellationTokenSource())
                using (var timeout = cts.TimeoutAfter(DatabasesLandlord.SystemConfiguration.DatabaseOperationTimeout))
                {
                    Etag lastProcessedDocEtag = null;

                    var batchSize = 0;
                    var batchDocCount = 0;
                    var processedDocuments = 0;
                    var hasMoreDocs = false;
                    var config = subscriptions.GetSubscriptionConfig(id);
                    var startEtag = config.AckEtag;
                    var criteria = config.Criteria;

                    bool isPrefixCriteria = !string.IsNullOrWhiteSpace(criteria.KeyStartsWith);

                    Func<JsonDocument, bool> addDocument = doc =>
                    {
                        processedDocuments++;
                        timeout.Delay();

                        // We cant continue because we have already maxed out the batch bytes size.
                        if (options.MaxSize.HasValue && batchSize >= options.MaxSize)
                            return false;

                        // We cant continue because we have already maxed out the amount of documents to send.
                        if (batchDocCount >= options.MaxDocCount)
                            return false;

                        // We can continue because we are ignoring system documents.
                        if (doc.Key.StartsWith("Raven/", StringComparison.InvariantCultureIgnoreCase))
                            return true;

                        // We can continue because we are ignoring the document as it doesn't fit the criteria.
                        if (MatchCriteria(criteria, doc) == false)
                            return true;

                        doc.ToJson().WriteTo(writer);
                        writer.WriteRaw(Environment.NewLine);

                        batchSize += doc.SerializedSizeOnDisk;
                        batchDocCount++;

                        return true; // We get the next document
                    };

                    int retries = 0;
                    do
                    {
                        int lastIndex = processedDocuments;

                        Database.TransactionalStorage.Batch(accessor =>
                        {
                            // we may be sending a LOT of documents to the user, and most 
                            // of them aren't going to be relevant for other ops, so we are going to skip
                            // the cache for that, to avoid filling it up very quickly
                            using (DocumentCacher.SkipSetAndGetDocumentsInDocumentCache())
                            {
                                if (isPrefixCriteria)
                                {
                                    // If we don't get any document from GetDocumentsWithIdStartingWith it could be that we are in presence of a lagoon of uninteresting documents, so we are hitting a timeout.
                                    lastProcessedDocEtag = Database.Documents.GetDocumentsWithIdStartingWith(criteria.KeyStartsWith, options.MaxDocCount - batchDocCount, startEtag, cts.Token, addDocument);

                                    hasMoreDocs = false;
                                }
                                else
                                {
                                    // It doesn't matter if we match the criteria or not, the document has been already processed.
                                    lastProcessedDocEtag = Database.Documents.GetDocuments(-1, options.MaxDocCount - batchDocCount, startEtag, cts.Token, addDocument);

                                    // If we don't get any document from GetDocuments it may be a signal that something is wrong.
                                    if (lastProcessedDocEtag == null)
                                    {
                                        hasMoreDocs = false;
                                    }
                                    else
                                    {
                                        var lastDocEtag = accessor.Staleness.GetMostRecentDocumentEtag();
                                        hasMoreDocs = EtagUtil.IsGreaterThan(lastDocEtag, lastProcessedDocEtag);

                                        startEtag = lastProcessedDocEtag;
                                    }

                                    retries = lastIndex == batchDocCount ? retries : 0;
                                }
                            }
                        });

                        if (lastIndex == processedDocuments)
                        {
                            if (retries == 3)
                            {
                                log.Warn("Subscription processing did not end up replicating any documents for 3 times in a row, stopping operation", retries);
                            }
                            else
                            {
                                log.Warn("Subscription processing did not end up replicating any documents, due to possible storage error, retry number: {0}", retries);
                            }
                            retries++;
                        }
                    }
                    while (retries < 3 && hasMoreDocs && batchDocCount < options.MaxDocCount && (options.MaxSize.HasValue == false || batchSize < options.MaxSize));

                    writer.WriteEndArray();

                    if (batchDocCount > 0 || isPrefixCriteria)
                    {
                        writer.WritePropertyName("LastProcessedEtag");
                        writer.WriteValue(lastProcessedDocEtag.ToString());

                        sentDocuments = true;
                    }

                    writer.WriteEndObject();
                    writer.Flush();

                    bufferStream.Flush();
                }
            }

            if (sentDocuments)
                subscriptions.UpdateBatchSentTime(id);
        }*/

        /*    private static bool MatchCriteria(SubscriptionCriteria criteria, JsonDocument doc)
        {
            if (criteria.BelongsToAnyCollection != null &&
                criteria.BelongsToAnyCollection.Contains(doc.Metadata.Value<string>(Constants.RavenEntityName), StringComparer.InvariantCultureIgnoreCase) == false)
                return false;

            if (criteria.KeyStartsWith != null && doc.Key.StartsWith(criteria.KeyStartsWith) == false)
                return false;

            if (criteria.PropertiesMatch != null)
            {
                foreach (var match in criteria.PropertiesMatch)
                {
                    var tokens = doc.DataAsJson.SelectTokenWithRavenSyntaxReturningFlatStructure(match.Key).Select(x => x.Item1).ToArray();

                    foreach (var curVal in tokens)
                    {
                        if (RavenJToken.DeepEquals(curVal, match.Value) == false)
                            return false;
                    }

                    if (tokens.Length == 0)
                        return false;
                }
            }

            if (criteria.PropertiesNotMatch != null)
            {
                foreach (var match in criteria.PropertiesNotMatch)
                {
                    var tokens = doc.DataAsJson.SelectTokenWithRavenSyntaxReturningFlatStructure(match.Key).Select(x => x.Item1).ToArray();

                    foreach (var curVal in tokens)
                    {
                        if (RavenJToken.DeepEquals(curVal, match.Value) == true)
                            return false;
                    }
                }
            }

            return true;
        }*/
    }
}