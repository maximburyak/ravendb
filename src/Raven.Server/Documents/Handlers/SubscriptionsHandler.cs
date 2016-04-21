using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Abstractions.Logging;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Voron.Data.Tables;
using Sparrow.Json.Parsing;
using Sparrow.Json;
using System.Net.WebSockets;
using System.Diagnostics;
using System.Threading;
using System.IO;
using System.Net.Http.Headers;
using Raven.Database.Extensions;
using Sparrow;
using Sparrow.Binary;


namespace Raven.Server.Documents.Handlers
{
    public class SubscriptionsHandler : DatabaseRequestHandler
    {

        private static readonly ILog log = LogManager.GetLogger(typeof(SubscriptionsHandler));

        [RavenAction("/databases/*/subscriptions/create", "POST", "/databases/{databaseName:string}/subscriptions/create")]
        public async Task Create()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var subscriptionCriteriaRaw = await context.ReadForDiskAsync(RequestBodyStream(), null);
                var subscriptionId = Database.SubscriptionStorage.CreateSubscription(subscriptionCriteriaRaw);
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

        [RavenAction("/databases/*/subscriptions", "DELETE",
            "/databases/{databaseName:string}/subscriptions?id={subscriptionId:long}")]
        public Task Delete()
        {
            var ids = HttpContext.Request.Query["id"];
            if (ids.Count == 0)
                throw new ArgumentException("The 'id' query string parameter is mandatory");

            long id;
            if (long.TryParse(ids[0], out id) == false)
                throw new ArgumentException("The 'id' query string parameter must be a valid long");


            Database.SubscriptionStorage.DeleteSubscription(id);

            HttpContext.Response.StatusCode = 204; // NoContent

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/subscriptions/open", "POST",
            "/databases/{databaseName:string}/subscriptions/open?id={subscriptionId:long}")]
        public Task Open()
        {
            var id = GetLongQueryString("id");

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                Database.SubscriptionStorage.AssertSubscriptionConfigExists(id);
                var options = context.ReadForDisk(RequestBodyStream(), "Subscriptions");
                Database.SubscriptionStorage.OpenSubscription(id, options);
            }

            return Task.CompletedTask;
        }



        private unsafe BlittableJsonReaderObject GetReaderObjectFromTableReader(TableValueReader tbl, int index, DocumentsOperationContext context)
        {
            int criteriaSize;
            var criteriaPtr = tbl.Read(index, out criteriaSize);
            return new BlittableJsonReaderObject(criteriaPtr, criteriaSize, context);
        }

        private unsafe long GetLongValueFromTableReader(TableValueReader tbl, int index)
        {
            int size;
            return *(long*)tbl.Read(index, out size);
        }

        private bool MatchCriteria(SubscriptionCriteria criteria, Document doc)
        {
            // todo: implement
            return true;
        }

        public static long DocumentsPullTimeoutMiliseconds = 1000;

        private static Task<WebSocketReceiveResult> _webSocketReceiveCompletedTask = Task.FromResult((WebSocketReceiveResult)null);


        private async Task<WebSocketReceiveResult> ReadFromWebSocketWithKeepAlives(WebSocket ws, ArraySegment<byte> clientAckBuffer, MemoryStream ms)
        {
            var receiveAckTask = ws.ReceiveAsync(clientAckBuffer, Database.DatabaseShutdown);
            while (Task.WhenAny(receiveAckTask, Task.Delay(5000)) != null &&
                (receiveAckTask.IsCompleted || receiveAckTask.IsFaulted ||
                    receiveAckTask.IsCanceled) == false)
            {
                ms.WriteByte((byte)'\r');
                ms.WriteByte((byte)'\n');
                // just to keep the heartbeat
                await FlushStreamToClient(ms, ws, Database.DatabaseShutdown);
            }

            return await receiveAckTask;
        }
        
        [RavenAction("/databases/*/subscriptions/close", "POST",
            "/databases/{databaseName:string}/subscriptions/close?id={subscriptionId:long}&connection={connection:string}&force={force:bool|optional}"
            )]
        public async Task Close()
        {
            var id = GetLongQueryString("id");
            var connection = GetStringQueryString("connection");
            bool force = GetBoolValueQueryString("force",required:false);

            if (force == false)
            {
                try
                {
                    Database.SubscriptionStorage.AssertOpenSubscriptionConnection(id, connection);
                }
                catch (SubscriptionException)
                {
                    // ignore if assertion exception happened on close
                    return;
                }
            }

            Database.SubscriptionStorage.ReleaseSubscription(id, force);
            

            return;
        }

        [RavenAction("/databases/*/subscriptions/pull", "GET",
            "/databases/{databaseName:string}/subscriptions/pull?id={subscriptionId:long}&connection={connection:string}"
            )]
        public async Task Pull()
        {
            var id = GetLongQueryString("id");
            var connection = GetStringQueryString("connection");
            Database.SubscriptionStorage.AssertOpenSubscriptionConnection(id, connection);

            var waitForMoreDocuments = new AsyncManualResetEvent();

            var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync();

            long lastEtagAcceptedFromClient = 0;
            long lastEtagSentToClient =0;

            try
            {
                DocumentsOperationContext context;
                using (ContextPool.AllocateOperationContext(out context))
                {
                    Log.Debug("Starting subscription pushing");
                    var clientAckBuffer = new ArraySegment<byte>(context.GetManagedBuffer());

                    var ackParserState = new JsonParserState();
                    using (var ms = new MemoryStream())
                    using (var writer = new BlittableJsonTextWriter(context, ms))
                    using (var ackParser = new UnmanagedJsonParser(context, ackParserState, string.Empty))
                    {
                        var config = Database.SubscriptionStorage.GetSubscriptionConfig(id);
                        var options = Database.SubscriptionStorage.GetSubscriptionOptions(id);

                        var startEtag = GetLongValueFromTableReader(config,
                            SubscriptionStorage.Schema.SubscriptionTable.AckEtagIndex);
                        var criteria = Database.SubscriptionStorage.GetCriteria(id, context);

                        Action<DocumentChangeNotification> registerNotification = notification =>
                        {
                            if (notification.CollectionName == criteria.Collection)
                            {
                                var sp = Stopwatch.StartNew();
                                waitForMoreDocuments.SetByAsyncCompletion();
                                Console.ForegroundColor = ConsoleColor.Red;
                                Console.WriteLine($"Notification Waited for {sp.ElapsedMilliseconds}");
                            }
                        };
                        Database.Notifications.OnDocumentChange += registerNotification;
                        try
                        {
                            int skipNumber = 0;
                            while (Database.DatabaseShutdown.IsCancellationRequested == false)
                            {
                                int documentsSent = 0;

                                var hasDocuments = false;
                                using (context.OpenReadTransaction())
                                {
                                    var documents = Database.DocumentsStorage.GetDocumentsAfter(context,
                                        criteria.Collection,
                                        startEtag+1, 0, options.MaxDocCount);

                                    foreach (var doc in documents)
                                    {
                                        hasDocuments = true;
                                        startEtag = doc.Etag;
                                        if (MatchCriteria(criteria, doc) == false)
                                        {
                                            if (skipNumber++ % options.MaxDocCount == 0)
                                            {
                                                ms.WriteByte((byte)'\r');
                                                ms.WriteByte((byte)'\n');
                                            }
                                            continue;
                                        }
                                        documentsSent++;
                                        doc.EnsureMetadata();
                                        context.Write(writer, doc.Data);
                                        lastEtagSentToClient = doc.Etag;
                                        doc.Data.Dispose();
                                    }
                                }

                                if (hasDocuments == false)
                                {
                                    while (await waitForMoreDocuments.WaitAsync(TimeSpan.FromSeconds(5)) == false)
                                    {
                                        ms.WriteByte((byte)'\r');
                                        ms.WriteByte((byte)'\n');
                                        // just to keep the heartbeat
                                        await FlushStreamToClient(ms, webSocket, Database.DatabaseShutdown);
                                    }
                                    
                                    waitForMoreDocuments.Reset();
                                    continue;
                                }
                                writer.Flush();
                                await FlushStreamToClient(ms, webSocket, Database.DatabaseShutdown, true);
                                Database.SubscriptionStorage.UpdateSubscriptionTimes(id, updateLastBatch: true,
                                    updateClientActivity: false);

                                if (documentsSent > 0)
                                {
                                    while (lastEtagAcceptedFromClient < lastEtagSentToClient)
                                    {
                                        using (var builder = new BlittableJsonDocumentBuilder(context,
                                                BlittableJsonDocumentBuilder.UsageMode.None, string.Empty, ackParser,
                                                ackParserState))
                                        {
                                            builder.ReadObject();

                                            while (builder.Read() == false)
                                            {
                                                var result =
                                                    await
                                                        ReadFromWebSocketWithKeepAlives(webSocket, clientAckBuffer, ms);
                                                ackParser.SetBuffer(new ArraySegment<byte>(clientAckBuffer.Array, 0,
                                                    result.Count));
                                            }

                                            builder.FinalizeDocument();

                                            using (var reader = builder.CreateReader())
                                            {
                                                if (reader.TryGet("LastEtag", out lastEtagAcceptedFromClient) == false)
                                                    // ReSharper disable once NotResolvedInText
                                                    throw new ArgumentNullException("LastEtag");
                                            }
                                        }
                                    }
                                    
                                    Database.SubscriptionStorage.UpdateSubscriptionTimes(id, updateLastBatch: false,
                                        updateClientActivity: true);
                                    Database.SubscriptionStorage.AcknowledgeBatchProcessed(id, startEtag);
                                }
                            }
                        }
                        finally
                        {
                            Database.Notifications.OnDocumentChange -= registerNotification;
                        }


                    }
                    
                }
            }
            catch (Exception e)
            {
                try
                {
                    var cancellationCts = new CancellationTokenSource();
                    var shudownTimeout = new CancellationTimeout(cancellationCts,
                        TimeSpan.FromSeconds(5));
                    await webSocket.CloseAsync(WebSocketCloseStatus.InternalServerError,
                        e.ToString(), Database.DatabaseShutdown);
                }
                catch
                {
                    // ignored
                }
                
                Log.ErrorException($"Failure in subscription id {id}", e);
            }
            finally
            {
                webSocket.Dispose();
            }
        }

        private static async Task FlushStreamToClient(MemoryStream ms, WebSocket webSocket, CancellationToken ct, bool endMessage = false)
        {
            ArraySegment<byte> bytes;
            ms.TryGetBuffer(out bytes);
            await webSocket.SendAsync(bytes, WebSocketMessageType.Text, endMessage, ct);
            ms.SetLength(0);
        }

        [RavenAction("/databases/*/subscriptions", "GET", "/databases/{databaseName:string}/subscriptions?start={start:int}&pageSize={pageSize:int}")]
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
                    Database.SubscriptionStorage.WriteSubscriptionTableValues(writer, context, subscriptionTableValues);
                }
            }
            HttpContext.Response.StatusCode = 200;
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/subscriptions/client-alive", "PATCH", "/databases/{databaseName:string}/subscriptions/client-alive?id={id:string}&connection={connection:string}")]
        public Task ClientAlive()
        {
            var id = GetLongQueryString("id");
            var connection = GetStringQueryString("connection");

            Database.SubscriptionStorage.AssertOpenSubscriptionConnection(id, connection);
            Database.SubscriptionStorage.UpdateSubscriptionTimes(id, updateClientActivity: true, updateLastBatch: false);
            return Task.CompletedTask;
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