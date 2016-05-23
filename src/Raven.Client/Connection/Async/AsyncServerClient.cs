//-----------------------------------------------------------------------
// <copyright file="AsyncServerClient.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Abstractions;
using Raven.Abstractions.Cluster;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Json;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Implementation;
using Raven.Client.Connection.Profiling;
using Raven.Client.Connection.Request;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Client.Listeners;
using Raven.Client.Metrics;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Globalization;
using Raven.Client.Util.Auth;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Client.Data.Queries;
using Raven.Client.Indexing;

namespace Raven.Client.Connection.Async
{
    public class AsyncServerClient : IAsyncDatabaseCommands, IAsyncInfoDatabaseCommands
    {
        private readonly ProfilingInformation profilingInformation;

        private readonly IDocumentConflictListener[] conflictListeners;

        private readonly Guid? sessionId;

        private readonly Func<AsyncServerClient, string, ClusterBehavior, bool, IRequestExecuter> requestExecuterGetter;

        private readonly Func<string, RequestTimeMetric> requestTimeMetricGetter;

        private readonly IRequestTimeMetric requestTimeMetric;

        private readonly string databaseName;

        private readonly string primaryUrl;

        private readonly OperationCredentials credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication;

        private readonly IRequestExecuter requestExecuter;

        internal readonly DocumentConvention convention;

        internal readonly HttpJsonRequestFactory jsonRequestFactory;

        private int requestCount;

        private NameValueCollection operationsHeaders = new NameValueCollection();

        public string Url => primaryUrl;

        public IRequestExecuter RequestExecuter => requestExecuter;

        public OperationCredentials PrimaryCredentials => credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication;

        public ClusterBehavior ClusterBehavior { get; private set; }

        public AsyncServerClient(
            string url,
            DocumentConvention convention,
            OperationCredentials credentials,
            HttpJsonRequestFactory jsonRequestFactory,
            Guid? sessionId,
            Func<AsyncServerClient, string, ClusterBehavior, bool, IRequestExecuter> requestExecuterGetter,
            Func<string, RequestTimeMetric> requestTimeMetricGetter,
            string databaseName,
            IDocumentConflictListener[] conflictListeners,
            bool incrementReadStripe,
            ClusterBehavior clusterBehavior)
        {
            profilingInformation = ProfilingInformation.CreateProfilingInformation(sessionId);
            primaryUrl = url;
            if (primaryUrl.EndsWith("/"))
                primaryUrl = primaryUrl.Substring(0, primaryUrl.Length - 1);
            this.jsonRequestFactory = jsonRequestFactory;
            this.sessionId = sessionId;
            this.convention = convention;
            credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication = credentials;
            this.databaseName = databaseName;
            this.conflictListeners = conflictListeners;
            ClusterBehavior = clusterBehavior;

            this.requestExecuterGetter = requestExecuterGetter;
            requestExecuter = requestExecuterGetter(this, databaseName, clusterBehavior, incrementReadStripe);
            requestExecuter.UpdateReplicationInformationIfNeededAsync(this);

            this.requestTimeMetricGetter = requestTimeMetricGetter;
            requestTimeMetric = requestTimeMetricGetter(databaseName);
        }

        private IRequestTimeMetric GetRequestTimeMetric(string operationUrl)
        {
            return string.Equals(operationUrl, Url, StringComparison.OrdinalIgnoreCase) ? requestTimeMetric : new DecreasingTimeMetric(requestTimeMetric);
        }

        public void Dispose()
        {
        }

        public Task<string[]> GetIndexNamesAsync(int start, int pageSize, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Get, async operationMetadata =>
            {
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url.IndexNames(start, pageSize), HttpMethod.Get, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url))).AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url))
                {
                    var json = (RavenJArray)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    return json.Select(x => x.Value<string>()).ToArray();
                }
            }, token);
        }

        public Task<IndexDefinition[]> GetIndexesAsync(int start, int pageSize, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Get, async operationMetadata =>
            {
                var operationUrl = operationMetadata.Url.IndexDefinition(null, start, pageSize);
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationUrl, HttpMethod.Get, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url))))
                {
                    request.AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url);

                    var json = (RavenJArray)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    return json.Deserialize<IndexDefinition[]>(convention);
                }
            }, token);
        }

        public Task<TransformerDefinition[]> GetTransformersAsync(int start, int pageSize, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Get, async operationMetadata =>
            {
                var operationUrl = operationMetadata.Url + "/transformers?start=" + start + "&pageSize=" + pageSize;
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationUrl, HttpMethod.Get, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url))))
                {
                    request.AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url);

                    var json = (RavenJArray)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);

                    //NOTE: To review, I'm not confidence this is the correct way to deserialize the transformer definition
                    return json.Select(x => JsonConvert.DeserializeObject<TransformerDefinition>(((RavenJObject)x)["definition"].ToString(), new JsonToJsonConverter())).ToArray();
                }
            }, token);
        }

        public Task SetTransformerLockAsync(string name, TransformerLockMode lockMode, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Post, async operationMetadata =>
            {
                var operationUrl = operationMetadata.Url + "/transformers/" + name + "?op=" + "lockModeChange" + "&mode=" + lockMode;
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationUrl, HttpMethod.Post, operationMetadata.Credentials, convention)))
                {
                    request.AddOperationHeaders(OperationsHeaders);
                    request.AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url);

                    return await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                }
            }, token);
        }

        internal Task ReplicateIndexAsync(string name, CancellationToken token = default(CancellationToken))
        {
            var url = String.Format("/replication/replicate-indexes?indexName={0}", Uri.EscapeDataString(name));

            using (var request = CreateRequest(url, HttpMethods.Post))
                return request.ExecuteRawResponseAsync();
        }

        public Task ResetIndexAsync(string name, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethods.Reset, async operationMetadata =>
            {
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url.Indexes(name), HttpMethods.Reset, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url))))
                {
                    request.AddOperationHeaders(OperationsHeaders);
                    request.AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url);

                    return await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                }
            }, token);
        }

        public Task SetIndexLockAsync(string name, IndexLockMode mode, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Post, async operationMetadata =>
            {
                var operationUrl = operationMetadata.Url.SetIndexLock(name, mode);
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationUrl, HttpMethod.Post, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url))))
                {
                    request.AddOperationHeaders(OperationsHeaders);
                    request.AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url);

                    return await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                }
            }, token);
        }
        public Task SetIndexPriorityAsync(string name, IndexingPriority priority, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Post, async operationMetadata =>
                {
                    var operationUrl = operationMetadata.Url.SetIndexPriority(name, priority);
                    using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationUrl, HttpMethod.Post, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url))))
                    {
                        request.AddOperationHeaders(OperationsHeaders);
                        request.AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url);

                        return await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    }
                }, token);
        }
        public Task<string> PutIndexAsync<TDocument, TReduceResult>(string name, IndexDefinitionBuilder<TDocument, TReduceResult> indexDef, CancellationToken token = default(CancellationToken))
        {
            return PutIndexAsync(name, indexDef, false, token);
        }

        public Task<string> PutIndexAsync<TDocument, TReduceResult>(string name,
                     IndexDefinitionBuilder<TDocument, TReduceResult> indexDef, bool overwrite = false, CancellationToken token = default(CancellationToken))
        {
            return PutIndexAsync(name, indexDef.ToIndexDefinition(convention), overwrite, token);
        }

        public Task<bool> IndexHasChangedAsync(string name, IndexDefinition indexDef, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Post, operationMetadata => DirectIndexHasChangedAsync(name, indexDef, operationMetadata, token), token);
        }

        private async Task<bool> DirectIndexHasChangedAsync(string name, IndexDefinition indexDef, OperationMetadata operationMetadata, CancellationToken token)
        {
            var requestUri = operationMetadata.Url.Indexes(name) + "?op=hasChanged";
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUri, HttpMethod.Post, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url)).AddOperationHeaders(OperationsHeaders)))
            {
                request.AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url);

                var serializeObject = JsonConvert.SerializeObject(indexDef, Default.Converters);

                await request.WriteAsync(serializeObject).WithCancellation(token).ConfigureAwait(false);
                var result = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return result.Value<bool>("Changed");
            }
        }

        public Task<string> PutIndexAsync(string name, IndexDefinition indexDef, CancellationToken token = default(CancellationToken))
        {
            return PutIndexAsync(name, indexDef, false, token);
        }

        public Task<string> PutIndexAsync(string name, IndexDefinition indexDef, bool overwrite, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Put, operationMetadata => DirectPutIndexAsync(name, indexDef, overwrite, operationMetadata, token), token);
        }

        public Task<string[]> PutIndexesAsync(IndexToAdd[] indexesToAdd, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Put, operationMetadata => DirectPutIndexesAsync(indexesToAdd, operationMetadata, token), token);
        }

        public Task<string[]> PutSideBySideIndexesAsync(IndexToAdd[] indexesToAdd, long? minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Put, operationMetadata => DirectPutSideBySideIndexesAsync(indexesToAdd, operationMetadata, minimumEtagBeforeReplace, replaceTimeUtc, token), token);
        }

        public Task<string> PutTransformerAsync(string name, TransformerDefinition transformerDefinition, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Put, operationMetadata => DirectPutTransformerAsync(name, transformerDefinition, operationMetadata, token), token);
        }

        public async Task<string> DirectPutIndexAsync(string name, IndexDefinition indexDef, bool overwrite, OperationMetadata operationMetadata, CancellationToken token = default(CancellationToken))
        {
            var requestUri = operationMetadata.Url + "/indexes?name=" + Uri.EscapeUriString(name) + "&definition=yes";
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUri, HttpMethod.Get, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url)).AddOperationHeaders(OperationsHeaders)))
            {
                request.AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url);

                try
                {
                    await request.ExecuteRequestAsync().WithCancellation(token).ConfigureAwait(false);
                    if (overwrite == false)
                        throw new InvalidOperationException("Cannot put index: " + name + ", index already exists");
                }
                catch (ErrorResponseException e)
                {
                    if (e.StatusCode != HttpStatusCode.NotFound)
                        throw;
                }
            }

            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUri, HttpMethod.Put, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url)).AddOperationHeaders(OperationsHeaders)))
            {
                var serializeObject = JsonConvert.SerializeObject(indexDef, Default.Converters);

                ErrorResponseException responseException;
                try
                {
                    await request.WriteAsync(serializeObject).ConfigureAwait(false);
                    var result = await request.ReadResponseJsonAsync().ConfigureAwait(false);
                    return result.Value<string>("Index");
                }
                catch (ErrorResponseException e)
                {
                    if (e.StatusCode != HttpStatusCode.BadRequest) throw;
                    responseException = e;
                }
                var error = await responseException.TryReadErrorResponseObject(new { Error = "", Message = "", IndexDefinitionProperty = "", ProblematicText = "" }).ConfigureAwait(false);
                if (error == null) throw responseException;

                throw new IndexCompilationException(error.Message) { IndexDefinitionProperty = error.IndexDefinitionProperty, ProblematicText = error.ProblematicText };
            }
        }

        public async Task<string[]> DirectPutIndexesAsync(IndexToAdd[] indexesToAdd, OperationMetadata operationMetadata, CancellationToken token = default(CancellationToken))
        {
            var requestUri = operationMetadata.Url + "/indexes";
            return await PutIndexes(operationMetadata, token, requestUri, indexesToAdd).ConfigureAwait(false);
        }

        public async Task<string[]> DirectPutSideBySideIndexesAsync(IndexToAdd[] indexesToAdd, OperationMetadata operationMetadata, long? minimumEtagBeforeReplace, DateTime? replaceTimeUtc, CancellationToken token = default(CancellationToken))
        {
            var sideBySideIndexes = new SideBySideIndexes
            {
                IndexesToAdd = indexesToAdd,
                MinimumEtagBeforeReplace = minimumEtagBeforeReplace,
                ReplaceTimeUtc = replaceTimeUtc
            };

            var requestUri = operationMetadata.Url + "/side-by-side-indexes";
            return await PutIndexes(operationMetadata, token, requestUri, sideBySideIndexes).ConfigureAwait(false);
        }

        private async Task<string[]> PutIndexes(OperationMetadata operationMetadata, CancellationToken token, string requestUri, object obj)
        {
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUri, HttpMethod.Put, operationMetadata.Credentials, convention).AddOperationHeaders(OperationsHeaders)))
            {
                var serializeObject = JsonConvert.SerializeObject(obj, Default.Converters);

                ErrorResponseException responseException;
                try
                {
                    await request.WriteAsync(serializeObject).WithCancellation(token).ConfigureAwait(false);
                    var result = await request.ReadResponseJsonAsync().ConfigureAwait(false);
                    return result
                        .Value<RavenJArray>("Indexes")
                        .Select(x => x.Value<string>())
                        .ToArray();
                }
                catch (ErrorResponseException e)
                {
                    if (e.StatusCode != HttpStatusCode.BadRequest)
                        throw;
                    responseException = e;
                }
                var error = await responseException.TryReadErrorResponseObject(new { Error = "", Message = "", IndexDefinitionProperty = "", ProblematicText = "" }).ConfigureAwait(false);
                if (error == null)
                    throw responseException;

                throw new IndexCompilationException(error.Message) { IndexDefinitionProperty = error.IndexDefinitionProperty, ProblematicText = error.ProblematicText };
            }
        }

        public async Task<string> DirectPutTransformerAsync(string name, TransformerDefinition transformerDefinition,
                                                            OperationMetadata operationMetadata, CancellationToken token = default(CancellationToken))
        {
            var requestUri = operationMetadata.Url + "/transformers/" + name;

            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUri, HttpMethod.Put, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url)).AddOperationHeaders(OperationsHeaders)))
            {
                var serializeObject = JsonConvert.SerializeObject(transformerDefinition, Default.Converters);

                ErrorResponseException responseException;
                try
                {
                    await request.WriteAsync(serializeObject).ConfigureAwait(false);
                    var result = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    return result.Value<string>("Transformer");
                }
                catch (BadRequestException e)
                {
                    throw new TransformCompilationException(e.Message);
                }
                catch (ErrorResponseException e)
                {
                    if (e.StatusCode != HttpStatusCode.BadRequest)
                        throw;
                    responseException = e;
                }
                var error = await responseException.TryReadErrorResponseObject(new { Error = "", Message = "" }).ConfigureAwait(false);
                if (error == null)
                    throw responseException;

                throw new TransformCompilationException(error.Message);
            }
        }

        public Task DeleteIndexAsync(string name, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Delete, async operationMetadata =>
            {
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url.Indexes(name), HttpMethod.Delete, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url)).AddOperationHeaders(operationsHeaders)))
                {
                    request.AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url);
                    await request.ExecuteRequestAsync().WithCancellation(token).ConfigureAwait(false);
                }
            }, token);
        }

        public Task DeleteCollectionAsync(string collectionName, CancellationToken token = default(CancellationToken))
        {
            EnsureIsNotNullOrEmpty(collectionName, nameof(collectionName));
            return ExecuteWithReplication(HttpMethod.Delete, async operationMetadata =>
            {
                string path = operationMetadata.Url.CollectionsDocs(collectionName);

                token.ThrowCancellationIfNotDefault(); //maybe the operation is canceled and we can spare the request..
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, path, HttpMethod.Delete, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url)).AddOperationHeaders(OperationsHeaders)))
                {
                    request.AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url);
                    await request.ExecuteRequestAsync().WithCancellation(token).ConfigureAwait(false);
                }
            }, token);
        }

        public Task<Operation> DeleteByIndexAsync(string indexName, IndexQuery queryToDelete, QueryOperationOptions options = null, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Delete, async operationMetadata =>
            {
                var notNullOptions = options ?? new QueryOperationOptions();
                string path = queryToDelete.GetIndexQueryUrl(operationMetadata.Url, indexName, "queries") + "&allowStale=" + notNullOptions.AllowStale
                     + "&details=" + notNullOptions.RetrieveDetails;
                if (notNullOptions.MaxOpsPerSecond != null)
                    path += "&maxOpsPerSec=" + notNullOptions.MaxOpsPerSecond;
                if (notNullOptions.StaleTimeout != null)
                    path += "&staleTimeout=" + notNullOptions.StaleTimeout;

                token.ThrowCancellationIfNotDefault(); //maybe the operation is canceled and we can spare the request..
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, path, HttpMethod.Delete, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url)).AddOperationHeaders(OperationsHeaders)))
                {
                    request.AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url);
                    RavenJToken jsonResponse;
                    try
                    {
                        jsonResponse = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    }
                    catch (ErrorResponseException e)
                    {
                        if (e.StatusCode == HttpStatusCode.NotFound) throw new InvalidOperationException("There is no index named: " + indexName, e);
                        throw;
                    }

                    // Be compatible with the response from v2.0 server
                    var opId = ((RavenJObject)jsonResponse)["OperationId"];

                    if (opId == null || opId.Type != JTokenType.Integer) return null;

                    return new Operation(this, opId.Value<long>());
                }
            }, token);
        }

        public Task DeleteTransformerAsync(string name, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Delete, async operationMetadata =>
            {
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url.Transformer(name), HttpMethod.Delete, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url)).AddOperationHeaders(operationsHeaders)))
                {
                    request.AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url);
                    await request.ExecuteRequestAsync().WithCancellation(token).ConfigureAwait(false);
                }
            }, token);
        }

        public Task<RavenJObject> PatchAsync(string key, PatchRequest patch, CancellationToken token = new CancellationToken())
        {
            return PatchAsync(key, patch, (long?)null, token);
        }

        public async Task<RavenJObject> PatchAsync(string key, PatchRequest patch, bool ignoreMissing, CancellationToken token = default(CancellationToken))
        {
            var batchResults = await BatchAsync(new ICommandData[]
            {
                new PatchCommandData
                {
                    Key = key,
                    Patch = patch,
                }
            }, token).ConfigureAwait(false);
            if (!ignoreMissing && batchResults[0].PatchResult != null &&
                batchResults[0].PatchResult == PatchResult.DocumentDoesNotExists)
                throw new DocumentDoesNotExistsException("Document with key " + key + " does not exist.");
            return batchResults[0].AdditionalData;
        }

        public async Task<RavenJObject> PatchAsync(string key, PatchRequest patch, long? etag, CancellationToken token = default(CancellationToken))
        {
            var batchResults = await BatchAsync(new ICommandData[]
            {
                new PatchCommandData
                {
                    Key = key,
                    Patch = patch,
                    Etag = etag
                }
            }, token).ConfigureAwait(false);
            return batchResults[0].AdditionalData;
        }

        public async Task<RavenJObject> PatchAsync(string key, PatchRequest patchExisting,
                                                   PatchRequest patchDefault,
                                                   CancellationToken token = default(CancellationToken))
        {
            var batchResults = await BatchAsync(new ICommandData[]
            {
                new PatchCommandData
                {
                    Key = key,
                    Patch = patchExisting,
                    PatchIfMissing = patchDefault,
                }
            }, token).ConfigureAwait(false);
            return batchResults[0].AdditionalData;
        }

        public Task<PutResult> PutAsync(string key, long? etag, RavenJObject document, RavenJObject metadata, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Put, operationMetadata => DirectPutAsync(operationMetadata, key, etag, document, metadata, token), token);
        }

        private async Task<PutResult> DirectPutAsync(OperationMetadata operationMetadata, string key, long? etag, RavenJObject document, RavenJObject metadata, CancellationToken token = default(CancellationToken))
        {
            if (metadata != null)
                document[Constants.Metadata] = metadata;

            var method = string.IsNullOrEmpty(key) ? HttpMethod.Post : HttpMethod.Put;

            if (key != null)
                key = Uri.EscapeDataString(key);

            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url.Doc(key), method, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url), etag: etag).AddOperationHeaders(OperationsHeaders)))
            {
                request.AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url);

                ErrorResponseException responseException;
                try
                {
                    await request.WriteAsync(document).WithCancellation(token).ConfigureAwait(false);
                    var result = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    if (result == null)
                    {
                        throw new InvalidOperationException("Got null response from the server after doing a put on " + key + ", something is very wrong. Probably a garbled response.");
                    }
                    return convention.CreateSerializer().Deserialize<PutResult>(new RavenJTokenReader(result));
                }
                catch (ErrorResponseException e)
                {
                    if (e.StatusCode != HttpStatusCode.Conflict) throw;
                    responseException = e;
                }
                throw FetchConcurrencyException(responseException);
            }
        }

        public IAsyncDatabaseCommands ForDatabase(string database, ClusterBehavior? clusterBehavior = null)
        {
            return ForDatabaseInternal(database, clusterBehavior);
        }

        public IAsyncDatabaseCommands ForSystemDatabase()
        {
            return ForSystemDatabaseInternal();
        }

        internal AsyncServerClient ForDatabaseInternal(string database, ClusterBehavior? clusterBehavior = null)
        {
            if (database == Constants.SystemDatabase)
                return ForSystemDatabaseInternal();

            var requestedClusterBehavior = clusterBehavior ?? convention.ClusterBehavior;

            var databaseUrl = MultiDatabase.GetRootDatabaseUrl(Url).ForDatabase(database);
            if (databaseUrl == Url && ClusterBehavior == requestedClusterBehavior)
                return this;

            return new AsyncServerClient(databaseUrl, convention, credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, jsonRequestFactory, sessionId, requestExecuterGetter, requestTimeMetricGetter, database, conflictListeners, false, requestedClusterBehavior) { operationsHeaders = operationsHeaders };
        }

        internal AsyncServerClient ForSystemDatabaseInternal()
        {
            var databaseUrl = MultiDatabase.GetRootDatabaseUrl(Url);
            if (databaseUrl == Url)
                return this;

            return new AsyncServerClient(databaseUrl, convention, credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, jsonRequestFactory, sessionId, requestExecuterGetter, requestTimeMetricGetter, databaseName, conflictListeners, false, ClusterBehavior.None) { operationsHeaders = operationsHeaders };
        }

        public NameValueCollection OperationsHeaders
        {
            get { return operationsHeaders; }
            set { operationsHeaders = value; }
        }

        public IAsyncGlobalAdminDatabaseCommands GlobalAdmin => new AsyncAdminServerClient(this);

        public IAsyncAdminDatabaseCommands Admin => new AsyncAdminServerClient(this);

        public Task<JsonDocument> GetAsync(string key, bool metadataOnly = false, CancellationToken token = default(CancellationToken))
        {
            EnsureIsNotNullOrEmpty(key, "key");

            return ExecuteWithReplication(HttpMethod.Get, async operationMetadata =>
            {
                var multiLoadResult = await DirectGetAsync(operationMetadata, new[] { key }, null, null, null, metadataOnly, token).ConfigureAwait(false);
                if (multiLoadResult.Results.Count == 0)
                    return null;
                return SerializationHelper.RavenJObjectToJsonDocument(multiLoadResult.Results[0]);
            }, token);
        }

        public Task<TransformerDefinition> GetTransformerAsync(string name, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Get, async operationMetadata =>
            {
                try
                {
                    using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url.Transformer(name), HttpMethod.Get, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url))).AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url))
                    {
                        var transformerDefinitionJson = (RavenJObject)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                        var value = transformerDefinitionJson.Value<RavenJObject>("Transformer");
                        return convention.CreateSerializer().Deserialize<TransformerDefinition>(new RavenJTokenReader(value));
                    }
                }
                catch (ErrorResponseException we)
                {
                    if (we.StatusCode == HttpStatusCode.NotFound)
                        return null;

                    throw;
                }
            }, token);
        }

        public Task<IndexDefinition> GetIndexAsync(string name, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Get, async operationMetadata =>
            {
                try
                {
                    using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url.IndexDefinition(name), HttpMethod.Get, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url))).AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url))
                    {
                        var json = (RavenJArray)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                        return json.Deserialize<IndexDefinition[]>(convention)[0];
                    }
                }
                catch (ErrorResponseException we)
                {
                    if (we.StatusCode == HttpStatusCode.NotFound)
                        return null;

                    throw;
                }

            }, token);
        }

        public Task<IndexPerformanceStats[]> GetIndexPerformanceStatisticsAsync(string[] indexNames = null)
        {
            return ExecuteWithReplication(HttpMethod.Get, async operationMetadata =>
            {
                var url = operationMetadata.Url.IndexPerformanceStats(indexNames);
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, url, HttpMethod.Get, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url))).AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url))
                {
                    var indexingPerformanceStatisticsJson = (RavenJArray)await request.ReadResponseJsonAsync().ConfigureAwait(false);
                    var results = new IndexPerformanceStats[indexingPerformanceStatisticsJson.Length];
                    for (var i = 0; i < indexingPerformanceStatisticsJson.Length; i++)
                    {
                        var stats = (RavenJObject)indexingPerformanceStatisticsJson[i];
                        results[i] = stats.Deserialize<IndexPerformanceStats>(convention);
                    }

                    return results;
                }
            });
        }

        public Task<LoadResult> GetAsync(string[] keys, string[] includes, string transformer = null,
                                              Dictionary<string, RavenJToken> transformerParameters = null, bool metadataOnly = false, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Get, operationMetadata => DirectGetAsync(operationMetadata, keys, includes, transformer, transformerParameters, metadataOnly, token), token);
        }

        private async Task<LoadResult> DirectGetAsync(OperationMetadata operationMetadata, string[] keys, string[] includes, string transformer,
                                                           Dictionary<string, RavenJToken> transformerParameters, bool metadataOnly, CancellationToken token = default(CancellationToken))
        {
            var pathBuilder = new StringBuilder(operationMetadata.Url);
            pathBuilder.Append("/docs?");

            if (metadataOnly)
                pathBuilder.Append("&metadata-only=true");

            includes.ApplyIfNotNull(include => pathBuilder.AppendFormat("&include={0}", include));

            if (string.IsNullOrEmpty(transformer) == false)
                pathBuilder.AppendFormat("&transformer={0}", transformer);

            transformerParameters.ApplyIfNotNull(tp => pathBuilder.AppendFormat("&tp-{0}={1}", tp.Key, tp.Value));

            var uniqueIds = new HashSet<string>(keys);
            // if it is too big, we drop to POST (note that means that we can't use the HTTP cache any longer)
            // we are fine with that, requests to load > 128 items are going to be rare
            var isGet = uniqueIds.Sum(x => x.Length) < 1024;
            var method = isGet ? HttpMethod.Get : HttpMethod.Post;
            if (isGet)
            {
                uniqueIds.ApplyIfNotNull(id => pathBuilder.AppendFormat("&id={0}", Uri.EscapeDataString(id)));
            }

            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, pathBuilder.ToString(), method, operationMetadata.Credentials, convention)
                .AddOperationHeaders(OperationsHeaders))
                .AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url))
            {
                if (isGet == false)
                {
                    await request.WriteAsync(new RavenJArray(uniqueIds)).WithCancellation(token).ConfigureAwait(false);
                }

                var result = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return await CompleteMultiGetAsync(operationMetadata, keys, includes, transformer, transformerParameters, result, token).ConfigureAwait(false);
            }
        }

        private async Task<LoadResult> CompleteMultiGetAsync(OperationMetadata operationMetadata, string[] keys, string[] includes, string transformer,
                                                           Dictionary<string, RavenJToken> transformerParameters, RavenJToken result, CancellationToken token = default(CancellationToken))
        {
            ErrorResponseException responseException;
            try
            {
                var uniqueKeys = new HashSet<string>(keys);

                var results = result
                    .Value<RavenJArray>("Results")
                    .Select(x => x as RavenJObject)
                    .ToList();

                var documents = results
                    .Where(x => x != null && x.ContainsKey("@metadata") && x["@metadata"].Value<string>("@id") != null)
                    .ToDictionary(x => x["@metadata"].Value<string>("@id"), x => x, StringComparer.OrdinalIgnoreCase);

                if (results.Count >= uniqueKeys.Count)
                {
                    for (var i = 0; i < uniqueKeys.Count; i++)
                    {
                        var key = keys[i];
                        if (documents.ContainsKey(key))
                            continue;

                        documents.Add(key, results[i]);
                    }
                }

                var multiLoadResult = new LoadResult
                {
                    Includes = result.Value<RavenJArray>("Includes").Cast<RavenJObject>().ToList(),
                    Results = documents.Count == 0 ? results : keys.Select(key => documents.ContainsKey(key) ? documents[key] : null).ToList()
                };

                var docResults = multiLoadResult.Results.Concat(multiLoadResult.Includes);

                return
                    await
                    RetryOperationBecauseOfConflict(operationMetadata, docResults, multiLoadResult,
                                                    () => DirectGetAsync(operationMetadata, keys, includes, transformer, transformerParameters, false, token), token: token).ConfigureAwait(false);
            }
            catch (ErrorResponseException e)
            {
                if (e.StatusCode != HttpStatusCode.Conflict)
                    throw;
                responseException = e;
            }
            throw FetchConcurrencyException(responseException);
        }

        public Task<JsonDocument[]> GetDocumentsAsync(int start, int pageSize, bool metadataOnly = false, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Get, async operationMetadata =>
            {
                var result = await GetDocumentsInternalAsync(start, null, pageSize, operationMetadata, metadataOnly, token).ConfigureAwait(false);

                return result.Cast<RavenJObject>()
                             .ToJsonDocuments()
                             .ToArray();
            }, token);
        }

        public Task<JsonDocument[]> GetDocumentsAsync(long? fromEtag, int pageSize, bool metadataOnly = false, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Get, async operationMetadata =>
            {
                var result = await GetDocumentsInternalAsync(null, fromEtag, pageSize, operationMetadata, metadataOnly, token).ConfigureAwait(false);
                return result.Cast<RavenJObject>()
                             .ToJsonDocuments()
                             .ToArray();
            }, token);
        }

        public async Task<RavenJArray> GetDocumentsInternalAsync(int? start, long? fromEtag, int pageSize, OperationMetadata operationMetadata, bool metadataOnly = false, CancellationToken token = default(CancellationToken))
        {
            var requestUri = Url + "/docs?";
            if (start.HasValue && start.Value > 0)
            {
                requestUri += "start=" + start;
            }
            else if (fromEtag != null)
            {
                requestUri += "etag=" + fromEtag;
            }
            requestUri += "&pageSize=" + pageSize;
            if (metadataOnly)
                requestUri += "&metadata-only=true";
            var @params = new CreateHttpJsonRequestParams(this, requestUri, HttpMethod.Get, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url))
                .AddOperationHeaders(OperationsHeaders);

            using (var request = jsonRequestFactory.CreateHttpJsonRequest(@params))
            {
                return (RavenJArray)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
            }
        }

        public Task<Operation> UpdateByIndexAsync(string indexName, IndexQuery queryToUpdate, PatchRequest patch, QueryOperationOptions options = null, CancellationToken token = default(CancellationToken))
        {
            var notNullOptions = options ?? new QueryOperationOptions();
            var requestData = RavenJObject.FromObject(patch).ToString(Formatting.Indented);
            return UpdateByIndexImpl(indexName, queryToUpdate, notNullOptions, requestData, token);
        }

        public Task<QueryResult> MoreLikeThisAsync(MoreLikeThisQuery query, CancellationToken token = default(CancellationToken))
        {
            var requestUrl = query.GetRequestUri();
            EnsureIsNotNullOrEmpty(requestUrl, "url");
            return ExecuteWithReplication(HttpMethod.Get, async operationMetadata =>
            {
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url + requestUrl, HttpMethod.Get, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url)).AddOperationHeaders(OperationsHeaders)))
                {
                    var json = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    return json.JsonDeserialization<QueryResult>();
                }
            }, token);
        }

        public Task<long> NextIdentityForAsync(string name, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Post, async operationMetadata =>
            {
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/identity/next?name=" + Uri.EscapeDataString(name), HttpMethod.Post, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url)).AddOperationHeaders(OperationsHeaders)))
                {
                    var readResponseJson = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    return readResponseJson.Value<long>("Value");
                }
            }, token);
        }

        public Task<long> SeedIdentityForAsync(string name, long value, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Post, async operationMetadata =>
            {
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/identity/seed?name=" + Uri.EscapeDataString(name) + "&value=" + Uri.EscapeDataString(value.ToString()), HttpMethod.Post, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url)).AddOperationHeaders(OperationsHeaders)))
                {
                    var readResponseJson = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);

                    return readResponseJson.Value<long>("Value");
                }
            }, token);
        }

        private Task<Operation> UpdateByIndexImpl(string indexName, IndexQuery queryToUpdate, QueryOperationOptions options, string requestData, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethods.Patch, async operationMetadata =>
            {
                var notNullOptions = options ?? new QueryOperationOptions();
                string path = queryToUpdate.GetIndexQueryUrl(operationMetadata.Url, indexName, "queries") + "&allowStale=" + notNullOptions.AllowStale
                    + "&maxOpsPerSec=" + notNullOptions.MaxOpsPerSecond + "&details=" + notNullOptions.RetrieveDetails;
                if (notNullOptions.StaleTimeout != null)
                    path += "&staleTimeout=" + notNullOptions.StaleTimeout;
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, path, HttpMethods.Patch, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url))))
                {
                    request.AddOperationHeaders(OperationsHeaders);
                    await request.WriteAsync(requestData).ConfigureAwait(false);

                    RavenJToken jsonResponse;
                    try
                    {
                        jsonResponse = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    }
                    catch (ErrorResponseException e)
                    {
                        if (e.StatusCode == HttpStatusCode.NotFound) throw new InvalidOperationException("There is no index named: " + indexName);
                        throw;
                    }

                    return new Operation(this, jsonResponse.Value<long>("OperationId"));
                }
            }, token);
        }

        public Task<FacetResults> GetFacetsAsync(string index, IndexQuery query, string facetSetupDoc, int start = 0,
                                                 int? pageSize = null, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Get, async operationMetadata =>
            {
                var requestUri = operationMetadata.Url + string.Format("/facets/{0}?facetDoc={1}{2}&facetStart={3}&facetPageSize={4}",
                Uri.EscapeUriString(index),
                Uri.EscapeDataString(facetSetupDoc),
                query.GetMinimalQueryString(),
                start,
                pageSize);

                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUri, HttpMethod.Get, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url)).AddOperationHeaders(OperationsHeaders)))
                {
                    request.AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url);

                    var cachedRequestDetails = jsonRequestFactory.ConfigureCaching(requestUri, (key, val) => request.AddHeader(key, val));
                    request.CachedRequestDetails = cachedRequestDetails.CachedRequest;
                    request.SkipServerCheck = cachedRequestDetails.SkipServerCheck;

                    var json = (RavenJObject)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    return json.JsonDeserialization<FacetResults>();
                }
            }, token);
        }

        public Task<FacetResults[]> GetMultiFacetsAsync(FacetQuery[] facetedQueries, CancellationToken token = default(CancellationToken))
        {
            var multiGetReuestItems = facetedQueries.Select(x =>
            {
                string addition;
                if (x.FacetSetupDoc != null)
                {
                    addition = "facetDoc=" + x.FacetSetupDoc;
                    return new GetRequest()
                    {
                        Url = "/facets/" + x.IndexName,
                        Query = string.Format("{0}&facetStart={1}&facetPageSize={2}&{3}",
                            x.Query.GetQueryString(),
                            x.PageStart,
                            x.PageSize,
                            addition)
                    };
                }

                var serializedFacets = SerializeFacetsToFacetsJsonString(x.Facets);
                if (serializedFacets.Length < (32 * 1024) - 1)
                {
                    addition = "facets=" + Uri.EscapeDataString(serializedFacets);
                    return new GetRequest()
                    {
                        Url = "/facets/" + x.IndexName,
                        Query = string.Format("{0}&facetStart={1}&facetPageSize={2}&{3}",
                            x.Query.GetQueryString(),
                            x.PageStart,
                            x.PageSize,
                            addition)
                    };
                }

                return new GetRequest()
                {
                    Url = "/facets/" + x.IndexName,
                    Query = string.Format("{0}&facetStart={1}&facetPageSize={2}",
                            x.Query.GetQueryString(),
                            x.PageStart,
                            x.PageSize),
                    Method = "POST",
                    Content = serializedFacets
                };
            }).ToArray();

            var results = MultiGetAsync(multiGetReuestItems, token).ContinueWith(x =>
            {
                var facetResults = new FacetResults[x.Result.Length];

                var getResponses = x.Result;
                for (var facetResultCounter = 0; facetResultCounter < facetResults.Length; facetResultCounter++)
                {
                    var getResponse = getResponses[facetResultCounter];
                    if (getResponse.RequestHasErrors())
                    {
                        throw new InvalidOperationException("Got an error from server, status code: " + getResponse.Status +
                                                       Environment.NewLine + getResponse.Result);

                    }
                    var curFacetDoc = getResponse.Result;

                    facetResults[facetResultCounter] = curFacetDoc.JsonDeserialization<FacetResults>();
                }

                return facetResults;
            }, token);
            return results;
        }

        public Task<FacetResults> GetFacetsAsync(string index, IndexQuery query, List<Facet> facets, int start = 0,
                                                 int? pageSize = null,
                                                 CancellationToken token = default(CancellationToken))
        {
            var facetsJson = SerializeFacetsToFacetsJsonString(facets);
            var method = facetsJson.Length > 1024 ? HttpMethod.Post : HttpMethod.Get;
            if (method == HttpMethod.Post)
            {
                return GetMultiFacetsAsync(new[]
                {
                    new FacetQuery
                    {
                        Facets =  facets,
                        IndexName = index,
                        Query = query,
                        PageSize = pageSize,
                        PageStart = start
                    }
                }).ContinueWith(x => x.Result.FirstOrDefault());
            }
            return ExecuteWithReplication(method, async operationMetadata =>
            {
                var requestUri = operationMetadata.Url + string.Format("/facets/{0}?{1}&facetStart={2}&facetPageSize={3}",
                                                                Uri.EscapeUriString(index),
                                                                query.GetQueryString(),
                                                                start,
                                                                pageSize);

                if (method == HttpMethod.Get)
                    requestUri += "&facets=" + Uri.EscapeDataString(facetsJson);
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUri, method, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url)).AddOperationHeaders(OperationsHeaders)).AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url))
                {
                    if (method != HttpMethod.Get)
                    {
                        await request.WriteAsync(facetsJson).ConfigureAwait(false);
                    }

                    var json = await request.ReadResponseJsonAsync().ConfigureAwait(false);

                    return json.JsonDeserialization<FacetResults>();
                }
            });
        }

        internal static string SerializeFacetsToFacetsJsonString(List<Facet> facets)
        {
            var ravenJArray = (RavenJArray)RavenJToken.FromObject(facets, new JsonSerializer
            {
                NullValueHandling = NullValueHandling.Ignore,
                DefaultValueHandling = DefaultValueHandling.Ignore,
            });
            foreach (var facet in ravenJArray)
            {
                var obj = (RavenJObject)facet;
                if (obj.Value<string>("Name") == obj.Value<string>("DisplayName"))
                    obj.Remove("DisplayName");
                var jArray = obj.Value<RavenJArray>("Ranges");
                if (jArray != null && jArray.Length == 0)
                    obj.Remove("Ranges");
            }
            string facetsJson = ravenJArray.ToString(Formatting.None);
            return facetsJson;
        }

        public Task<LogItem[]> GetLogsAsync(bool errorsOnly, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Get, async operationMetadata =>
            {
                var requestUri = Url + "/logs";
                if (errorsOnly)
                    requestUri += "?type=error";

                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUri, HttpMethod.Get, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url))))
                {
                    request.AddOperationHeaders(OperationsHeaders);
                    request.AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url);

                    var result = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    return convention.CreateSerializer().Deserialize<LogItem[]>(new RavenJTokenReader(result));
                }
            }, token);
        }

        public async Task<LicensingStatus> GetLicenseStatusAsync(CancellationToken token = default(CancellationToken))
        {
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, (MultiDatabase.GetRootDatabaseUrl(Url) + "/license/status"), HttpMethod.Get, credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, convention, GetRequestTimeMetric(Url))))
            {
                request.AddOperationHeaders(OperationsHeaders);

                var result = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return convention.CreateSerializer().Deserialize<LicensingStatus>(new RavenJTokenReader(result));
            }
        }

        public async Task<BuildNumber> GetBuildNumberAsync(CancellationToken token = default(CancellationToken))
        {
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, (Url + "/build/version"), HttpMethod.Get, credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, convention, GetRequestTimeMetric(Url))))
            {
                request.AddOperationHeaders(OperationsHeaders);

                var result = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return convention.CreateSerializer().Deserialize<BuildNumber>(new RavenJTokenReader(result));
            }
        }

        public async Task<IndexMergeResults> GetIndexMergeSuggestionsAsync(CancellationToken token = default(CancellationToken))
        {
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, (Url + "/debug/suggest-index-merge"), HttpMethod.Get, credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, convention, GetRequestTimeMetric(Url))))
            {
                request.AddOperationHeaders(OperationsHeaders);

                var result = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return convention.CreateSerializer().Deserialize<IndexMergeResults>(new RavenJTokenReader(result));
            }
        }

        public Task<JsonDocument[]> StartsWithAsync(string keyPrefix, string matches, int start, int pageSize,
                                    RavenPagingInformation pagingInformation = null,
                                    bool metadataOnly = false,
                                    string exclude = null,
                                    string transformer = null,
                                    Dictionary<string, RavenJToken> transformerParameters = null,
                                    string skipAfter = null, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Get, async operationMetadata =>
            {
                var actualStart = start;

                var nextPage = pagingInformation != null && pagingInformation.IsForPreviousPage(start, pageSize);
                if (nextPage)
                    actualStart = pagingInformation.NextPageStart;

                var actualUrl = string.Format("{0}/docs?startsWith={1}&matches={5}&exclude={4}&start={2}&pageSize={3}", operationMetadata.Url,
                                              Uri.EscapeDataString(keyPrefix), actualStart.ToInvariantString(), pageSize.ToInvariantString(), exclude, matches);

                if (metadataOnly)
                    actualUrl += "&metadata-only=true";

                if (string.IsNullOrEmpty(skipAfter) == false)
                    actualUrl += "&skipAfter=" + Uri.EscapeDataString(skipAfter);

                if (string.IsNullOrEmpty(transformer) == false)
                {
                    actualUrl += "&transformer=" + transformer;

                    if (transformerParameters != null)
                    {
                        actualUrl = transformerParameters.Aggregate(actualUrl,
                                             (current, transformerParamater) =>
                                             current + ("&" + string.Format("tp-{0}={1}", transformerParamater.Key, transformerParamater.Value)));
                    }
                }

                if (nextPage)
                    actualUrl += "&next-page=true";

                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, actualUrl, HttpMethod.Get, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url)).AddOperationHeaders(OperationsHeaders)))
                {
                    request.AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url);

                    var result = (RavenJArray)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);

                    int nextPageStart;
                    if (pagingInformation != null && int.TryParse(request.ResponseHeaders[Constants.Headers.NextPageStart], out nextPageStart)) pagingInformation.Fill(start, pageSize, nextPageStart);

                    var docResults = result.OfType<RavenJObject>().ToList();
                    var startsWithResults = SerializationHelper.RavenJObjectsToJsonDocuments(docResults.Select(x => (RavenJObject)x.CloneToken())).ToArray();
                    return await RetryOperationBecauseOfConflict(operationMetadata, docResults, startsWithResults, () =>
                        StartsWithAsync(keyPrefix, matches, start, pageSize, pagingInformation, metadataOnly, exclude, transformer, transformerParameters, skipAfter, token), conflictedResultId =>
                            new ConflictException("Conflict detected on " + conflictedResultId.Substring(0, conflictedResultId.IndexOf("/conflicts/", StringComparison.OrdinalIgnoreCase)) +
                                ", conflict must be resolved before the document will be accessible")
                            { ConflictedVersionIds = new[] { conflictedResultId } }, token).ConfigureAwait(false);
                }
            }, token);
        }

        public Task<GetResponse[]> MultiGetAsync(GetRequest[] requests, CancellationToken token = default(CancellationToken))
        {
            return MultiGetAsyncInternal(requests, token, null);
        }

        private Task<GetResponse[]> MultiGetAsyncInternal(GetRequest[] requests, CancellationToken token, Reference<OperationMetadata> operationMetadataRef)
        {
            return ExecuteWithReplication<GetResponse[]>(HttpMethod.Get, async operationMetadata => // logical GET even though the actual request is a POST
            {
                if (operationMetadataRef != null)
                    operationMetadataRef.Value = operationMetadata;
                var multiGetOperation = new MultiGetOperation(this, convention, operationMetadata.Url, requests);

                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, multiGetOperation.RequestUri, HttpMethod.Post, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url)).AddOperationHeaders(OperationsHeaders)))
                {
                    request.AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url);

                    var requestsForServer = multiGetOperation.PreparingForCachingRequest(jsonRequestFactory);

                    var postedData = JsonConvert.SerializeObject(requestsForServer);

                    if (multiGetOperation.CanFullyCache(jsonRequestFactory, request, postedData))
                    {
                        var cachedResponses = multiGetOperation.HandleCachingResponse(new GetResponse[requests.Length], jsonRequestFactory);
                        return cachedResponses;
                    }

                    await request.WriteAsync(postedData).ConfigureAwait(false);
                    var result = await request.ReadResponseJsonAsync().ConfigureAwait(false);
                    var responses = convention.CreateSerializer().Deserialize<GetResponse[]>(new RavenJTokenReader(result));

                    await multiGetOperation.TryResolveConflictOrCreateConcurrencyException(responses, (key, conflictDoc, etag) => TryResolveConflictOrCreateConcurrencyException(operationMetadata, key, conflictDoc, etag, token)).ConfigureAwait(false);

                    return multiGetOperation.HandleCachingResponse(responses, jsonRequestFactory);
                }
            }, token);
        }

        public Task<QueryResult> QueryAsync(string index, IndexQuery query, bool metadataOnly = false, bool indexEntriesOnly = false, CancellationToken token = default(CancellationToken))
        {
            var method = (query.Query == null || query.Query.Length <= convention.MaxLengthOfQueryUsingGetUrl)
                ? HttpMethod.Get : HttpMethod.Post;

            if (method == HttpMethod.Post)
            {
                return QueryAsyncAsPost(index, query, metadataOnly, indexEntriesOnly, token);
            }

            return QueryAsyncAsGet(index, query, metadataOnly, indexEntriesOnly, method, token);
        }

        private Task<QueryResult> QueryAsyncAsGet(string index, IndexQuery query, bool metadataOnly, bool indexEntriesOnly, HttpMethod method, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(method, async operationMetadata =>
            {
                EnsureIsNotNullOrEmpty(index, "index");
                string path = query.GetIndexQueryUrl(operationMetadata.Url, index, "queries", includeQuery: method == HttpMethod.Get);

                if (metadataOnly)
                    path += "&metadata-only=true";
                if (indexEntriesOnly)
                    path += "&debug=entries";

                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, path, method, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url)) { AvoidCachingRequest = query.DisableCaching }.AddOperationHeaders(OperationsHeaders)))
                {
                    RavenJObject json = null;
                    request.AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url);

                    json = (RavenJObject)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);


                    ErrorResponseException responseException;
                    try
                    {
                        if (json == null) throw new InvalidOperationException("Got empty response from the server for the following request: " + request.Url);

                        var queryResult = SerializationHelper.ToQueryResult(json, request.ResponseHeaders.GetEtagHeader(), request.ResponseHeaders.Get(Constants.Headers.RequestTime), request.Size);

                        if (request.ResponseStatusCode == HttpStatusCode.NotModified)
                            queryResult.DurationMilliseconds = -1;

                        var docResults = queryResult.Results.Concat(queryResult.Includes);
                        return await RetryOperationBecauseOfConflict(operationMetadata, docResults, queryResult, () =>
                            QueryAsync(index, query, metadataOnly, indexEntriesOnly, token), conflictedResultId =>
                                new ConflictException("Conflict detected on " + conflictedResultId.Substring(0, conflictedResultId.IndexOf("/conflicts/", StringComparison.OrdinalIgnoreCase)) +
                                    ", conflict must be resolved before the document will be accessible")
                                { ConflictedVersionIds = new[] { conflictedResultId } }, token).ConfigureAwait(false);
                    }
                    catch (ErrorResponseException e)
                    {
                        if (e.StatusCode == HttpStatusCode.NotFound)
                        {
                            var text = e.ResponseString;
                            if (text.Contains("maxQueryString")) throw new ErrorResponseException(e, text);
                            throw new ErrorResponseException(e, "There is no index named: " + index);
                        }
                        responseException = e;
                    }
                    if (HandleException(responseException)) return null;
                    throw responseException;
                }
            }, token);
        }

        private async Task<QueryResult> QueryAsyncAsPost(string index, IndexQuery query, bool metadataOnly, bool indexEntriesOnly, CancellationToken token = default(CancellationToken))
        {
            var stringBuilder = new StringBuilder();
            query.AppendQueryString(stringBuilder);

            if (metadataOnly)
                stringBuilder.Append("&metadata-only=true");
            if (indexEntriesOnly)
                stringBuilder.Append("&debug=entries");

            try
            {
                var operationMetadataRef = new Reference<OperationMetadata>();
                var result = await MultiGetAsyncInternal(new[]
                {
                    new GetRequest
                    {
                        Query = stringBuilder.ToString(),
                        Url = "/queries/" + index
                    }
                }, token, operationMetadataRef).ConfigureAwait(false);

                var json = (RavenJObject)result[0].Result;
                var queryResult = SerializationHelper.ToQueryResult(json, result[0].GetEtagHeader(), result[0].Headers[Constants.Headers.RequestTime], -1);

                var docResults = queryResult.Results.Concat(queryResult.Includes);
                return await RetryOperationBecauseOfConflict(operationMetadataRef.Value, docResults, queryResult,
                    () => QueryAsync(index, query, metadataOnly, indexEntriesOnly, token),
                    conflictedResultId => new ConflictException("Conflict detected on " + conflictedResultId.Substring(0, conflictedResultId.IndexOf("/conflicts/", StringComparison.OrdinalIgnoreCase)) +
                            ", conflict must be resolved before the document will be accessible")
                    { ConflictedVersionIds = new[] { conflictedResultId } },
                    token).ConfigureAwait(false);
            }
            catch (OperationCanceledException oce)
            {
                throw new TaskCanceledException(string.Format("Canceled Index {0} Query", index), oce);
            }
            catch (Exception e)
            {
                var errorResponseException = e as ErrorResponseException;

                if (errorResponseException != null)
                {
                    if (errorResponseException.StatusCode == HttpStatusCode.NotFound)
                    {
                        var text = errorResponseException.ResponseString;
                        if (text.Contains("maxQueryString")) throw new ErrorResponseException(errorResponseException, text);
                        throw new ErrorResponseException(errorResponseException, "There is no index named: " + index);
                    }

                    if (HandleException(errorResponseException)) return null;
                }

                throw;
            }
        }

        /// <summary>
        /// Attempts to handle an exception raised when receiving a response from the server
        /// </summary>
        /// <param name="e">The exception to handle</param>
        /// <returns>returns true if the exception is handled, false if it should be thrown</returns>
        private bool HandleException(ErrorResponseException e)
        {
            if (e.StatusCode == HttpStatusCode.InternalServerError)
            {
                var content = e.ResponseString;
                var json = RavenJObject.Load(new JsonTextReader(new StringReader(content)));
                var error = json.Deserialize<ServerRequestError>(convention);

                throw new ErrorResponseException(e, error.Error);
            }
            return false;
        }

        public Task<SuggestionQueryResult> SuggestAsync(string index, SuggestionQuery suggestionQuery, CancellationToken token = default(CancellationToken))
        {
            if (suggestionQuery == null)
                throw new ArgumentNullException("suggestionQuery");

            return ExecuteWithReplication(HttpMethod.Get, async operationMetadata =>
            {
                var requestUri = operationMetadata.Url + string.Format("/suggest/{0}?term={1}&field={2}&max={3}&popularity={4}",
                    Uri.EscapeUriString(index),
                    Uri.EscapeDataString(suggestionQuery.Term),
                    Uri.EscapeDataString(suggestionQuery.Field),
                    Uri.EscapeDataString(suggestionQuery.MaxSuggestions.ToInvariantString()),
                                                              suggestionQuery.Popularity);

                if (suggestionQuery.Accuracy.HasValue)
                    requestUri += "&accuracy=" + suggestionQuery.Accuracy.Value.ToInvariantString();

                if (suggestionQuery.Distance.HasValue)
                    requestUri += "&distance=" + suggestionQuery.Distance;

                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, requestUri, HttpMethod.Get, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url)).AddOperationHeaders(OperationsHeaders)))
                {
                    request.AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url);

                    var json = (RavenJObject)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    return new SuggestionQueryResult { Suggestions = ((RavenJArray)json["Suggestions"]).Select(x => x.Value<string>()).ToArray(), };
                }
            }, token);
        }

        public Task<BatchResult[]> BatchAsync(IEnumerable<ICommandData> commandDatas, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Post, async operationMetadata =>
            {
                var sp = Stopwatch.StartNew();
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/bulk_docs", HttpMethod.Post, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url)).AddOperationHeaders(OperationsHeaders)))
                {
                    request.AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url);

                    var serializedData = commandDatas.Select(x => x.ToJson()).ToList();
                    var jArray = new RavenJArray(serializedData);

                    ErrorResponseException responseException;
                    try
                    {
                        Console.ForegroundColor = ConsoleColor.Cyan;
                        Console.WriteLine("Client Bulk Before Write: " + sp.ElapsedMilliseconds);
                        sp.Restart();
                        Console.ForegroundColor = ConsoleColor.White;
                        await request.WriteAsync(jArray).WithCancellation(token).ConfigureAwait(false);
                        Console.ForegroundColor = ConsoleColor.Cyan;
                        Console.WriteLine("Client Bulk After Write: " + sp.ElapsedMilliseconds);
                        sp.Restart();
                        Console.ForegroundColor = ConsoleColor.White;
                        var response = (RavenJArray)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                        Console.ForegroundColor = ConsoleColor.Cyan;
                        Console.WriteLine("Client Bulk After Read: " + sp.ElapsedMilliseconds);
                        sp.Restart();
                        Console.ForegroundColor = ConsoleColor.White;
                        if (response == null)
                        {
                            throw new InvalidOperationException("Got null response from the server after doing a batch, something is very wrong. Probably a garbled response. Posted: " + jArray);
                        }
                        var batchResults = convention.CreateSerializer().Deserialize<BatchResult[]>(new RavenJTokenReader(response));
                        Console.ForegroundColor = ConsoleColor.Cyan;
                        Console.WriteLine("Client Bulk After Deserialize: " + sp.ElapsedMilliseconds);
                        sp.Restart();
                        Console.ForegroundColor = ConsoleColor.White;
                        return batchResults;
                    }
                    catch (ErrorResponseException e)
                    {
                        if (e.StatusCode != HttpStatusCode.Conflict) throw;
                        responseException = e;
                    }
                    throw FetchConcurrencyException(responseException);
                }
            }, token);
        }

        private static ConcurrencyException FetchConcurrencyException(ErrorResponseException e)
        {
            var text = e.ResponseString;
            var errorResults = JsonConvert.DeserializeAnonymousType(text, new
            {
                url = (string)null,
                actualETag = 0,
                expectedETag = 0,
                error = (string)null
            });
            return new ConcurrencyException(errorResults.error)
            {
                ActualETag = errorResults.actualETag,
                ExpectedETag = errorResults.expectedETag
            };
        }

        private static void EnsureIsNotNullOrEmpty(string key, string argName)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentException("Key cannot be null or empty", argName);
        }

        public async Task<DatabaseStatistics> GetStatisticsAsync(CancellationToken token = default(CancellationToken))
        {
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, Url.Stats(), HttpMethod.Get, credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, convention, GetRequestTimeMetric(Url))))
            {
                var json = (RavenJObject)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return json.Deserialize<DatabaseStatistics>(convention);
            }
        }

        public async Task<IndexErrors> GetIndexErrorsAsync(string name, CancellationToken token = default(CancellationToken))
        {
            var errors = await GetIndexErrorsAsync(new[] { name }, token).ConfigureAwait(false);
            return errors[0];
        }

        public async Task<IndexErrors[]> GetIndexErrorsAsync(IEnumerable<string> indexNames, CancellationToken token = default(CancellationToken))
        {
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, Url.IndexErrors(indexNames), HttpMethod.Get, credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, convention, GetRequestTimeMetric(Url))))
            {
                var json = (RavenJArray)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return json.Deserialize<IndexErrors[]>(convention);
            }
        }

        public Task<IndexErrors[]> GetIndexErrorsAsync(CancellationToken token = default(CancellationToken))
        {
            return GetIndexErrorsAsync(indexNames: null, token: token);
        }

        public async Task<IndexStats> GetIndexStatisticsAsync(string name, CancellationToken token = new CancellationToken())
        {
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, Url.IndexStatistics(name), HttpMethod.Get, credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, convention, GetRequestTimeMetric(Url))))
            {
                var json = (RavenJObject)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return json.Deserialize<IndexStats>(convention);
            }
        }

        public async Task<UserInfo> GetUserInfoAsync(CancellationToken token = default(CancellationToken))
        {
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, Url.UserInfo(), HttpMethod.Get, PrimaryCredentials, convention)))
            {
                var json = (RavenJObject)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return json.Deserialize<UserInfo>(convention);
            }
        }

        public async Task<UserPermission> GetUserPermissionAsync(string database, bool readOnly, CancellationToken token = default(CancellationToken))
        {
            if (string.IsNullOrEmpty(database))
            {
                throw new ArgumentException("database name cannot be null or empty");
            }

            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, Url.UserPermission(database, readOnly), HttpMethod.Get, PrimaryCredentials, convention)))
            {
                var json = (RavenJObject)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return json.Deserialize<UserPermission>(convention);
            }
        }

        public static string Static(string url, string key)
        {
            return url + "/static/" + Uri.EscapeUriString(key);
        }

        public IDisposable DisableAllCaching()
        {
            return jsonRequestFactory.DisableAllCaching();
        }

        public Task<string[]> GetTermsAsync(string index, string field, string fromValue, int pageSize, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Get, async operationMetadata =>
            {
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url.Terms(index, field, fromValue, pageSize), HttpMethod.Get, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url))).AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url))
                {
                    var result = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    var json = ((RavenJArray)result);
                    return json.Select(x => x.Value<string>()).ToArray();
                }
            }, token);
        }

        public ProfilingInformation ProfilingInformation => profilingInformation;

        public event EventHandler<FailoverStatusChangedEventArgs> FailoverStatusChanged
        {
            add { RequestExecuter.FailoverStatusChanged += value; }
            remove { RequestExecuter.FailoverStatusChanged -= value; }
        }

        public IDisposable ForceReadFromMaster()
        {
            return requestExecuter.ForceReadFromMaster();
        }

        public Task<long?> HeadAsync(string key, CancellationToken token = default(CancellationToken))
        {
            EnsureIsNotNullOrEmpty(key, "key");
            return ExecuteWithReplication(HttpMethod.Head, u => DirectHeadAsync(u, key, token), token);
        }

        public Task<IAsyncEnumerator<RavenJObject>> StreamQueryAsync(string index, IndexQuery query, Reference<QueryHeaderInformation> queryHeaderInfo, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Get, operationMetadata => DirectStreamQueryAsync(index, query, queryHeaderInfo, operationMetadata, token), token);
        }

        private async Task<IAsyncEnumerator<RavenJObject>> DirectStreamQueryAsync(string index, IndexQuery query, Reference<QueryHeaderInformation> queryHeaderInfo, OperationMetadata operationMetadata, CancellationToken cancellationToken = default(CancellationToken))
        {
            EnsureIsNotNullOrEmpty(index, "index");
            string path;
            HttpMethod method;
            if (query.Query != null && query.Query.Length > convention.MaxLengthOfQueryUsingGetUrl)
            {
                path = query.GetIndexQueryUrl(operationMetadata.Url, index, "streams/query", includePageSizeEvenIfNotExplicitlySet: false, includeQuery: false);
                method = HttpMethod.Post;
            }
            else
            {
                method = HttpMethod.Get;
                path = query.GetIndexQueryUrl(operationMetadata.Url, index, "streams/query", includePageSizeEvenIfNotExplicitlySet: false);
            }

            var request = jsonRequestFactory
                .CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, path, method, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url))
                .AddOperationHeaders(OperationsHeaders))
                .AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url);

            request.RemoveAuthorizationHeader();

            var tokenRetriever = new SingleAuthTokenRetriever(this, jsonRequestFactory, convention, OperationsHeaders, operationMetadata);

            var token = await tokenRetriever.GetToken().WithCancellation(cancellationToken).ConfigureAwait(false);
            try
            {
                token = await tokenRetriever.ValidateThatWeCanUseToken(token).WithCancellation(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                request.Dispose();

                throw new InvalidOperationException(
                    "Could not authenticate token for query streaming, if you are using ravendb in IIS make sure you have Anonymous Authentication enabled in the IIS configuration",
                    e);
            }
            request.AddOperationHeader("Single-Use-Auth-Token", token);

            HttpResponseMessage response;
            try
            {
                if (method == HttpMethod.Post)
                {
                    response = await request.ExecuteRawResponseAsync(query.Query)
                                            .WithCancellation(cancellationToken)
                                            .ConfigureAwait(false);
                }
                else
                {
                    response = await request.ExecuteRawResponseAsync()
                                            .WithCancellation(cancellationToken)
                                            .ConfigureAwait(false);
                }

                await response.AssertNotFailingResponse().WithCancellation(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                request.Dispose();

                if (index.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase) && request.ResponseStatusCode == HttpStatusCode.NotFound)
                {
                    throw new InvalidOperationException(
                        @"StreamQuery does not support querying dynamic indexes. It is designed to be used with large data-sets and is unlikely to return all data-set after 15 sec of indexing, like Query() does.",
                        e);
                }

                throw;
            }

            queryHeaderInfo.Value = new QueryHeaderInformation
            {
                Index = response.Headers.GetFirstValue("Raven-Index"),
                IndexTimestamp = DateTime.ParseExact(response.Headers.GetFirstValue("Raven-Index-Timestamp"), Default.DateTimeFormatsToRead,
                                                                CultureInfo.InvariantCulture, DateTimeStyles.None),
                IndexEtag = long.Parse(response.Headers.GetFirstValue("Raven-Index-Etag")),
                ResultEtag = long.Parse(response.Headers.GetFirstValue("Raven-Result-Etag")),
                IsStale = bool.Parse(response.Headers.GetFirstValue("Raven-Is-Stale")),
                TotalResults = int.Parse(response.Headers.GetFirstValue("Raven-Total-Results"))
            };

            return new YieldStreamResults(request, await response.GetResponseStreamWithHttpDecompression().ConfigureAwait(false));
        }

        public class YieldStreamResults : IAsyncEnumerator<RavenJObject>
        {
            private readonly HttpJsonRequest request;

            private readonly int start;

            private readonly int pageSize;

            private readonly RavenPagingInformation pagingInformation;

            private readonly Stream stream;
            private readonly StreamReader streamReader;
            private readonly JsonTextReaderAsync reader;
            private bool complete;

            private bool wasInitialized;
            private Func<JsonTextReaderAsync, bool> customizedEndResult;

            public YieldStreamResults(HttpJsonRequest request, Stream stream, int start = 0, int pageSize = 0, RavenPagingInformation pagingInformation = null, Func<JsonTextReaderAsync, bool> customizedEndResult = null)
            {
                this.request = request;
                this.start = start;
                this.pageSize = pageSize;
                this.pagingInformation = pagingInformation;
                this.stream = stream;
                this.customizedEndResult = customizedEndResult;
                streamReader = new StreamReader(stream);
                reader = new JsonTextReaderAsync(streamReader);
            }

            private async Task InitAsync()
            {
                if (await reader.ReadAsync().ConfigureAwait(false) == false || reader.TokenType != JsonToken.StartObject)
                    throw new InvalidOperationException("Unexpected data at start of stream");

                if (await reader.ReadAsync().ConfigureAwait(false) == false || reader.TokenType != JsonToken.PropertyName || Equals("Results", reader.Value) == false)
                    throw new InvalidOperationException("Unexpected data at stream 'Results' property name");

                if (await reader.ReadAsync().ConfigureAwait(false) == false || reader.TokenType != JsonToken.StartArray)
                    throw new InvalidOperationException("Unexpected data at 'Results', could not find start results array");
            }

            public void Dispose()
            {
                try
                {
                    reader.Close();
                }
                catch (Exception)
                {
                }
                try
                {
                    streamReader.Dispose();
                }
                catch (Exception)
                {

                }
                try
                {
                    stream.Dispose();
                }
                catch (Exception)
                {

                }
                try
                {
                    request.Dispose();
                }
                catch (Exception)
                {

                }
            }

            public async Task<bool> MoveNextAsync()
            {
                if (complete)
                {
                    // to parallel IEnumerable<T>, subsequent calls to MoveNextAsync after it has returned false should
                    // also return false, rather than throwing
                    return false;
                }

                if (wasInitialized == false)
                {
                    await InitAsync().ConfigureAwait(false);
                    wasInitialized = true;
                }

                if (await reader.ReadAsync().ConfigureAwait(false) == false)
                    throw new InvalidOperationException("Unexpected end of data");

                if (reader.TokenType == JsonToken.EndArray)
                {
                    complete = true;

                    await TryReadNextPageStart().ConfigureAwait(false);

                    await EnsureValidEndOfResponse().ConfigureAwait(false);
                    this.Dispose();
                    return false;
                }
                Current = (RavenJObject)await RavenJToken.ReadFromAsync(reader).ConfigureAwait(false);
                return true;
            }

            private async Task TryReadNextPageStart()
            {
                if (!(await reader.ReadAsync().ConfigureAwait(false)) || reader.TokenType != JsonToken.PropertyName)
                    return;

                switch ((string)reader.Value)
                {
                    case "NextPageStart":
                        var nextPageStart = await reader.ReadAsInt32().ConfigureAwait(false);
                        if (pagingInformation == null)
                            return;
                        if (nextPageStart.HasValue == false)
                            throw new InvalidOperationException("Unexpected end of data");

                        pagingInformation.Fill(start, pageSize, nextPageStart.Value);
                        break;
                    case "Error":
                        var err = await reader.ReadAsString().ConfigureAwait(false);
                        throw new InvalidOperationException("Server error" + Environment.NewLine + err);
                    default:
                        if (customizedEndResult != null && customizedEndResult(reader))
                            break;

                        throw new InvalidOperationException("Unexpected property name: " + reader.Value);
                }

            }

            private async Task EnsureValidEndOfResponse()
            {
                if (reader.TokenType != JsonToken.EndObject && await reader.ReadAsync().ConfigureAwait(false) == false)
                    throw new InvalidOperationException("Unexpected end of response - missing EndObject token");

                if (reader.TokenType != JsonToken.EndObject)
                    throw new InvalidOperationException(string.Format("Unexpected token type at the end of the response: {0}. Error: {1}", reader.TokenType, streamReader.ReadToEnd()));

                var remainingContent = await streamReader.ReadToEndAsync().ConfigureAwait(false);

                if (string.IsNullOrEmpty(remainingContent) == false)
                    throw new InvalidOperationException("Server error: " + remainingContent);
            }

            public RavenJObject Current { get; private set; }
        }

        public async Task<IAsyncEnumerator<RavenJObject>> StreamDocsAsync(
                        long? fromEtag = null, string startsWith = null,
                        string matches = null, int start = 0,
                        int pageSize = int.MaxValue,
                        string exclude = null,
                        RavenPagingInformation pagingInformation = null,
                        string skipAfter = null,
                        string transformer = null,
                        Dictionary<string, RavenJToken> transformerParameters = null,
                        CancellationToken token = default(CancellationToken))
        {
            if (fromEtag != null && startsWith != null)
                throw new InvalidOperationException("Either fromEtag or startsWith must be null, you can't specify both");

            if (fromEtag != null) // etags does not match between servers
                return await DirectStreamDocsAsync(fromEtag, null, matches, start, pageSize, exclude, pagingInformation, new OperationMetadata(Url, credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, null), skipAfter, transformer, transformerParameters, token).ConfigureAwait(false);

            return await ExecuteWithReplication(HttpMethod.Get, operationMetadata => DirectStreamDocsAsync(null, startsWith, matches, start, pageSize, exclude, pagingInformation, operationMetadata, skipAfter, transformer, transformerParameters, token), token).ConfigureAwait(false);
        }

        private async Task<IAsyncEnumerator<RavenJObject>> DirectStreamDocsAsync(long? fromEtag, string startsWith, string matches, int start, int pageSize, string exclude, RavenPagingInformation pagingInformation, OperationMetadata operationMetadata, string skipAfter, string transformer, Dictionary<string, RavenJToken> transformerParameters, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (fromEtag != null && startsWith != null)
                throw new InvalidOperationException("Either fromEtag or startsWith must be null, you can't specify both");

            var sb = new StringBuilder(operationMetadata.Url).Append("/streams/docs?");

            if (fromEtag != null)
            {
                sb.Append("etag=").Append(fromEtag).Append("&");
            }
            else
            {
                if (startsWith != null)
                {
                    sb.Append("startsWith=").Append(Uri.EscapeDataString(startsWith)).Append("&");
                }
                if (matches != null)
                {
                    sb.Append("matches=").Append(Uri.EscapeDataString(matches)).Append("&");
                }
                if (exclude != null)
                {
                    sb.Append("exclude=").Append(Uri.EscapeDataString(exclude)).Append("&");
                }
                if (skipAfter != null)
                {
                    sb.Append("skipAfter=").Append(Uri.EscapeDataString(skipAfter)).Append("&");
                }
            }

            if (string.IsNullOrEmpty(transformer) == false)
                sb.Append("transformer=").Append(Uri.EscapeDataString(transformer)).Append("&");

            if (transformerParameters != null && transformerParameters.Count > 0)
            {
                foreach (var pair in transformerParameters)
                {
                    var parameterName = pair.Key;
                    var parameterValue = pair.Value;

                    sb.AppendFormat("tp-{0}={1}", parameterName, parameterValue).Append("&");
                }
            }

            var actualStart = start;

            var nextPage = pagingInformation != null && pagingInformation.IsForPreviousPage(start, pageSize);
            if (nextPage)
                actualStart = pagingInformation.NextPageStart;

            if (actualStart != 0)
                sb.Append("start=").Append(actualStart).Append("&");

            if (pageSize != int.MaxValue)
                sb.Append("pageSize=").Append(pageSize).Append("&");

            if (nextPage)
                sb.Append("next-page=true").Append("&");

            var request = jsonRequestFactory
                .CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, sb.ToString(), HttpMethod.Get, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url))
                .AddOperationHeaders(OperationsHeaders))
                .AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url);

            request.RemoveAuthorizationHeader();

            var tokenRetriever = new SingleAuthTokenRetriever(this, jsonRequestFactory, convention, OperationsHeaders, operationMetadata);

            var token = await tokenRetriever.GetToken().WithCancellation(cancellationToken).ConfigureAwait(false);
            try
            {
                token = await tokenRetriever.ValidateThatWeCanUseToken(token).WithCancellation(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                request.Dispose();

                throw new InvalidOperationException(
                    "Could not authenticate token for query streaming, if you are using ravendb in IIS make sure you have Anonymous Authentication enabled in the IIS configuration",
                    e);
            }
            request.AddOperationHeader("Single-Use-Auth-Token", token);

            HttpResponseMessage response;

            try
            {
                response = await request.ExecuteRawResponseAsync()
                                        .WithCancellation(cancellationToken)
                                        .ConfigureAwait(false);

                await response.AssertNotFailingResponse().WithCancellation(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception)
            {
                request.Dispose();

                throw;
            }

            return new YieldStreamResults(request, await response.GetResponseStreamWithHttpDecompression().WithCancellation(cancellationToken).ConfigureAwait(false), start, pageSize, pagingInformation);
        }

        public Task DeleteAsync(string key, long? etag, CancellationToken token = default(CancellationToken))
        {
            EnsureIsNotNullOrEmpty(key, "key");
            return ExecuteWithReplication(HttpMethod.Delete, async operationMetadata =>
            {
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url.Doc(key), HttpMethod.Delete, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url)).AddOperationHeaders(operationsHeaders)))
                {
                    request.AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url);
                    if (etag != null)
                        request.AddHeader("If-None-Match", etag.ToString());

                    try
                    {
                        await request.ExecuteRequestAsync().WithCancellation(token).ConfigureAwait(false);
                    }
                    catch (ErrorResponseException e)
                    {
                        if (e.StatusCode != HttpStatusCode.Conflict)
                            throw;

                        throw FetchConcurrencyException(e);
                    }
                }
            }, token);
        }

        public string UrlFor(string documentKey)
        {
            return Url.Doc(documentKey);
        }

        public WebSocketBulkInsertOperation GetBulkInsertOperation(CancellationTokenSource cts = default(CancellationTokenSource))
        {
            return new WebSocketBulkInsertOperation(this, cts);
        }

        private async Task<long?> DirectHeadAsync(OperationMetadata operationMetadata, string key, CancellationToken token = default(CancellationToken))
        {
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url.Doc(key), HttpMethod.Head, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url)).AddOperationHeaders(OperationsHeaders)).AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url))
            {
                try
                {
                    await request.ExecuteRequestAsync().WithCancellation(token).ConfigureAwait(false);
                    var etag = request.ResponseHeaders[Constants.MetadataEtagField];
                    return HttpExtensions.EtagHeaderToEtag(etag);
                }
                catch (ErrorResponseException e)
                {
                    if (e.StatusCode == HttpStatusCode.NotFound)
                        return null;

                    if (e.StatusCode == HttpStatusCode.Conflict)
                    {
                        throw new ConflictException("Conflict detected on " + key + ", conflict must be resolved before the document will be accessible. Cannot get the conflicts ids because a HEAD request was performed. A GET request will provide more information, and if you have a document conflict listener, will automatically resolve the conflict") { Etag = e.Etag };
                    }
                    throw;
                }
            }
        }

        public Task<RavenJToken> ExecuteGetRequest(string requestUrl)
        {
            EnsureIsNotNullOrEmpty(requestUrl, "url");
            return ExecuteWithReplication(HttpMethod.Get, async operationMetadata =>
            {
                using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url + requestUrl, HttpMethod.Get, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url)).AddOperationHeaders(OperationsHeaders)))
                {
                    return await request.ReadResponseJsonAsync().ConfigureAwait(false);
                }
            });
        }

        public HttpJsonRequest CreateRequest(string requestUrl, HttpMethod method, bool disableRequestCompression = false, bool disableAuthentication = false, TimeSpan? timeout = null)
        {
            var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(this, Url + requestUrl, method, credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, convention, GetRequestTimeMetric(Url), timeout)
                .AddOperationHeaders(OperationsHeaders);
            createHttpJsonRequestParams.DisableRequestCompression = disableRequestCompression;
            createHttpJsonRequestParams.DisableAuthentication = disableAuthentication;
            return jsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams);
        }

        public HttpJsonRequest CreateReplicationAwareRequest(string currentServerUrl, string requestUrl, HttpMethod method, bool disableRequestCompression = false, bool disableAuthentication = false, TimeSpan? timeout = null)
        {
            var metadata = new RavenJObject();


            var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(this, currentServerUrl + requestUrl, method, credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication,
                                                                              convention, GetRequestTimeMetric(currentServerUrl), timeout).AddOperationHeaders(OperationsHeaders);
            createHttpJsonRequestParams.DisableRequestCompression = disableRequestCompression;
            createHttpJsonRequestParams.DisableAuthentication = disableAuthentication;

            return jsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams)
                                     .AddRequestExecuterAndReplicationHeaders(this, currentServerUrl);
        }

        public Task CommitAsync(string txId, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Post, operationMetadata => DirectCommit(txId, operationMetadata, token), token);
        }

        private async Task DirectCommit(string txId, OperationMetadata operationMetadata, CancellationToken token)
        {
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/transaction/commit?tx=" + txId, HttpMethod.Post, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url)).AddOperationHeaders(OperationsHeaders)).AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url))
            {
                await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
            }
        }

        public Task RollbackAsync(string txId, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Post, operationMetadata => DirectRollback(txId, operationMetadata, token), token);
        }

        private async Task DirectRollback(string txId, OperationMetadata operationMetadata, CancellationToken token = default(CancellationToken))
        {
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/transaction/rollback?tx=" + txId, HttpMethod.Post, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url)).AddOperationHeaders(OperationsHeaders)).AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url))
            {
                await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
            }
        }

        public Task PrepareTransactionAsync(string txId, Guid? resourceManagerId = null, byte[] recoveryInformation = null, CancellationToken token = default(CancellationToken))
        {
            return ExecuteWithReplication(HttpMethod.Post, operationMetadata => DirectPrepareTransaction(txId, operationMetadata, resourceManagerId, recoveryInformation, token), token);
        }

        private async Task DirectPrepareTransaction(string txId, OperationMetadata operationMetadata, Guid? resourceManagerId, byte[] recoveryInformation, CancellationToken token = default(CancellationToken))
        {
            var opUrl = operationMetadata.Url + "/transaction/prepare?tx=" + txId;
            if (resourceManagerId != null)
                opUrl += "&resourceManagerId=" + resourceManagerId;

            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, opUrl, HttpMethod.Post, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url))
                .AddOperationHeaders(OperationsHeaders))
                .AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url))
            {
                if (recoveryInformation != null)
                {
                    var ms = new MemoryStream(recoveryInformation);
                    await request.WriteAsync(ms).WithCancellation(token).ConfigureAwait(false);
                }

                await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
            }
        }

        internal Task ExecuteWithReplication(HttpMethod method, Func<OperationMetadata, Task> operation, CancellationToken token = default(CancellationToken))
        {
            // Convert the Func<string, Task> to a Func<string, Task<object>>
            return ExecuteWithReplication(method, u => operation(u).ContinueWith<object>(t =>
            {
                t.AssertNotFailed();
                return null;
            }, token), token);
        }

        private volatile bool currentlyExecuting;
        private volatile bool retryBecauseOfConflict;
        private bool resolvingConflict;
        private bool resolvingConflictRetries;

        internal async Task<T> ExecuteWithReplication<T>(HttpMethod method, Func<OperationMetadata, Task<T>> operation, CancellationToken token = default(CancellationToken))
        {
            var currentRequest = Interlocked.Increment(ref requestCount);
            if (currentlyExecuting && convention.AllowMultipleAsyncOperations == false && retryBecauseOfConflict == false)
                throw new InvalidOperationException("Only a single concurrent async request is allowed per async client instance.");
            currentlyExecuting = true;
            try
            {
                return await requestExecuter.ExecuteOperationAsync(this, method, currentRequest, operation, token).ConfigureAwait(false);
            }
            finally
            {
                currentlyExecuting = false;
            }
        }

        private async Task<bool> AssertNonConflictedDocumentAndCheckIfNeedToReload(OperationMetadata operationMetadata, RavenJObject docResult,
                                                                                    Func<string, ConflictException> onConflictedQueryResult = null, CancellationToken token = default(CancellationToken))
        {
            if (docResult == null)
                return (false);
            var metadata = docResult[Constants.Metadata];
            if (metadata == null)
                return (false);

            if (metadata.Value<int>("@Http-Status-Code") == 409)
            {
                var etag = HttpExtensions.EtagHeaderToEtag(metadata.Value<string>("@etag"));
                var e = await TryResolveConflictOrCreateConcurrencyException(operationMetadata, metadata.Value<string>("@id"), docResult, etag, token).ConfigureAwait(false);
                if (e != null)
                    throw e;
                return true;

            }

            if (metadata.Value<bool>(Constants.Headers.RavenReplicationConflict) && onConflictedQueryResult != null)
                throw onConflictedQueryResult(metadata.Value<string>("@id"));

            return (false);
        }

        private async Task<ConflictException> TryResolveConflictOrCreateConcurrencyException(OperationMetadata operationMetadata, string key,
                                                                                             RavenJObject conflictsDoc,
                                                                                             long? etag,
                                                                                             CancellationToken token)
        {
            var ravenJArray = conflictsDoc.Value<RavenJArray>("Conflicts");
            if (ravenJArray == null)
                throw new InvalidOperationException(
                    "Could not get conflict ids from conflicted document, are you trying to resolve a conflict when using metadata-only?");

            var conflictIds = ravenJArray.Select(x => x.Value<string>()).ToArray();

            var result = await TryResolveConflictByUsingRegisteredListenersAsync(key, etag, conflictIds, operationMetadata, token).ConfigureAwait(false);
            if (result)
                return null;

            return
                new ConflictException(
                    "Conflict detected on " + key + ", conflict must be resolved before the document will be accessible")
                {
                    ConflictedVersionIds = conflictIds,
                    Etag = etag
                };
        }

        internal async Task<bool> TryResolveConflictByUsingRegisteredListenersAsync(string key, long? etag, string[] conflictIds, OperationMetadata operationMetadata = null, CancellationToken token = default(CancellationToken))
        {
            if (operationMetadata == null)
                operationMetadata = new OperationMetadata(Url, credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, null);

            if (conflictListeners.Length > 0 && resolvingConflict == false)
            {
                resolvingConflict = true;
                try
                {
                    var result = await DirectGetAsync(operationMetadata, conflictIds, null, null, null, false, token).ConfigureAwait(false);
                    var results = result.Results.Select(SerializationHelper.ToJsonDocument).ToArray();

                    foreach (var conflictListener in conflictListeners)
                    {
                        JsonDocument resolvedDocument;
                        if (conflictListener.TryResolveConflict(key, results, out resolvedDocument))
                        {
                            await DirectPutAsync(operationMetadata, key, etag, resolvedDocument.DataAsJson, resolvedDocument.Metadata, token).ConfigureAwait(false);
                            return true;
                        }
                    }

                    return false;
                }
                finally
                {
                    resolvingConflict = false;
                }
            }

            return false;
        }

        private async Task<T> RetryOperationBecauseOfConflict<T>(OperationMetadata operationMetadata, IEnumerable<RavenJObject> docResults,
                                                                 T currentResult, Func<Task<T>> nextTry, Func<string, ConflictException> onConflictedQueryResult = null, CancellationToken token = default(CancellationToken))
        {
            bool requiresRetry = false;
            foreach (var docResult in docResults)
            {
                token.ThrowIfCancellationRequested();
                requiresRetry |=
                    await AssertNonConflictedDocumentAndCheckIfNeedToReload(operationMetadata, docResult, onConflictedQueryResult, token).ConfigureAwait(false);
            }

            if (!requiresRetry)
                return currentResult;

            if (resolvingConflictRetries)
                throw new InvalidOperationException(
                    "Encountered another conflict after already resolving a conflict. Conflict resolution cannot recurse.");
            resolvingConflictRetries = true;
            retryBecauseOfConflict = true;
            try
            {
                return await nextTry().WithCancellation(token).ConfigureAwait(false);
            }
            finally
            {
                resolvingConflictRetries = false;
                retryBecauseOfConflict = false;
            }
        }

        public async Task<RavenJToken> GetOperationStatusAsync(long id)
        {
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, Url + "/operations/status?id=" + id, HttpMethod.Get, credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, convention, GetRequestTimeMetric(Url)).AddOperationHeaders(OperationsHeaders)))
            {
                try
                {
                    return await request.ReadResponseJsonAsync().ConfigureAwait(false);
                }
                catch (ErrorResponseException e)
                {
                    if (e.StatusCode == HttpStatusCode.NotFound) return null;
                    throw;
                }
            }
        }

        public IAsyncInfoDatabaseCommands Info => this;

        async Task<ReplicationStatistics> IAsyncInfoDatabaseCommands.GetReplicationInfoAsync(CancellationToken token)
        {
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(this, Url.ReplicationInfo(), HttpMethod.Get, credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication, convention, GetRequestTimeMetric(Url))))
            {
                var json = (RavenJObject)await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return json.Deserialize<ReplicationStatistics>(convention);
            }
        }

        public IAsyncDatabaseCommands With(ICredentials credentialsForSession)
        {
            return WithInternal(credentialsForSession);
        }

        internal AsyncServerClient WithInternal(ICredentials credentialsForSession)
        {
            return new AsyncServerClient(Url, convention, new OperationCredentials(credentialsThatShouldBeUsedOnlyInOperationsWithoutReplication.ApiKey, credentialsForSession), jsonRequestFactory, sessionId,
                                         requestExecuterGetter, requestTimeMetricGetter, databaseName, conflictListeners, false, convention.ClusterBehavior);
        }

        internal async Task<ReplicationDocumentWithClusterInformation> DirectGetReplicationDestinationsAsync(OperationMetadata operationMetadata, TimeSpan? timeout = null)
        {
            var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(this, operationMetadata.Url + "/replication/topology", HttpMethod.Get, operationMetadata.Credentials, convention, GetRequestTimeMetric(operationMetadata.Url), timeout);
            using (var request = jsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams.AddOperationHeaders(OperationsHeaders)).AddRequestExecuterAndReplicationHeaders(this, operationMetadata.Url))
            {
                try
                {
                    var requestJson = await request.ReadResponseJsonAsync().ConfigureAwait(false);
                    return requestJson.JsonDeserialization<ReplicationDocumentWithClusterInformation>();
                }
                catch (ErrorResponseException e)
                {
                    switch (e.StatusCode)
                    {
                        case HttpStatusCode.NotFound:
                        case HttpStatusCode.BadRequest: //replication bundle if not enabled
                            return null;
                        default:
                            throw;
                    }
                }
            }
        }
    }
}
