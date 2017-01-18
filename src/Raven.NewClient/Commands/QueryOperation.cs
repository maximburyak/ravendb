using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Extensions;
using Raven.NewClient.Json.Utilities;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.NewClient.Client.Commands
{
    public class QueryOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;
        private readonly string _indexName;
        private readonly IndexQuery _indexQuery;
        private readonly bool _waitForNonStaleResults;
        private bool _disableEntitiesTracking;
        private readonly bool _metadataOnly;
        private readonly bool _indexEntriesOnly;
        private readonly TimeSpan? _timeout;
        private readonly Func<IndexQuery, IEnumerable<object>, IEnumerable<object>> _transformResults;
        private readonly HashSet<string> _includes;
        private QueryResult _currentQueryResults;
        private readonly string[] _projectionFields;
        private Stopwatch _sp;
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<QueryOperation>("Raven.NewClient.Client");

        public QueryResult CurrentQueryResults => _currentQueryResults;

        public QueryOperation(InMemoryDocumentSessionOperations session, string indexName, IndexQuery indexQuery,
                              string[] projectionFields, bool waitForNonStaleResults, TimeSpan? timeout,
                              Func<IndexQuery, IEnumerable<object>, IEnumerable<object>> transformResults,
                              HashSet<string> includes, bool disableEntitiesTracking, bool metadataOnly = false, bool indexEntriesOnly = false)
        {
            _session = session;
            _indexName = indexName;
            _indexQuery = indexQuery;
            _waitForNonStaleResults = waitForNonStaleResults;
            _timeout = timeout;
            _transformResults = transformResults;
            _includes = includes;
            _projectionFields = projectionFields;
            _disableEntitiesTracking = disableEntitiesTracking;
            _metadataOnly = metadataOnly;
            _indexEntriesOnly = indexEntriesOnly;

            AssertNotQueryById();
        }

        public QueryCommand CreateRequest()
        {
            _session.IncrementRequestCount();
            LogQuery();

            return new QueryCommand(_session.Conventions, _session.Context, _indexName, _indexQuery, _includes, _metadataOnly, _indexEntriesOnly);
        }

        public void SetResult(QueryResult queryResult)
        {
            EnsureIsAcceptableAndSaveResult(queryResult);
        }

        private static readonly Regex idOnly = new Regex(@"^__document_id \s* : \s* ([\w_\-/\\\.]+) \s* $",
            RegexOptions.Compiled |
            RegexOptions.IgnorePatternWhitespace);

        private void AssertNotQueryById()
        {
            // this applies to dynamic indexes only
            if (!_indexName.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase) &&
                !string.Equals(_indexName, "dynamic", StringComparison.OrdinalIgnoreCase))
                return;

            var match = idOnly.Match(_indexQuery.Query);
            if (match.Success == false)
                return;

            if (_session.Conventions.AllowQueriesOnId)
                return;

            var value = match.Groups[1].Value;

            throw new InvalidOperationException("Attempt to query by id only is blocked, you should use call session.Load(\"" + value + "\"); instead of session.Query().Where(x=>x.Id == \"" + value + "\");" + Environment.NewLine + "You can turn this error off by specifying documentStore.Conventions.AllowQueriesOnId = true;, but that is not recommend and provided for backward compatibility reasons only.");
        }

        private void StartTiming()
        {
            _sp = Stopwatch.StartNew();
        }

        public void LogQuery()
        {
            if (_logger.IsInfoEnabled)
                _logger.Info($"Executing query '{_indexQuery.Query}' on index '{_indexName}' in '{_session.StoreIdentifier}'");
        }

        public IDisposable EnterQueryContext()
        {
            StartTiming();

            if (_waitForNonStaleResults == false)
                return null;

            return _session.DocumentStore.DisableAggressiveCaching();
        }

        public IList<T> Complete<T>()
        {
            var queryResult = _currentQueryResults.CreateSnapshot();
            foreach (BlittableJsonReaderObject include in queryResult.Includes)
            {
                var newDocumentInfo = DocumentInfo.GetNewDocumentInfo(include);
                _session.includedDocumentsByKey[newDocumentInfo.Id] = newDocumentInfo;
            }

            var usedTransformer = string.IsNullOrEmpty(_indexQuery.Transformer) == false;
            var list = new List<T>();
            foreach (BlittableJsonReaderObject document in queryResult.Results)
            {
                if (usedTransformer)
                {
                    BlittableJsonReaderArray values;
                    if (document.TryGet("$values", out values) == false)
                        throw new InvalidOperationException("Transformed document must have a $values property");

                    list.AddRange(TransformerHelpers.ParseValuesFromBlittableArray<T>(_session, values));

                    continue;
                }

                var metadata = document.GetMetadata();

                string id;
                metadata.TryGetId(out id);

                list.Add(Deserialize<T>(id, document, metadata));
            }

            if (_disableEntitiesTracking == false)
                _session.RegisterMissingIncludes(queryResult.Results, _includes);

            if (_transformResults == null)
                return list;

            return _transformResults(_indexQuery, list.Cast<object>()).Cast<T>().ToList();
        }

        private T Deserialize<T>(string id, BlittableJsonReaderObject document, BlittableJsonReaderObject metadata)
        {
            if (_projectionFields == null || _projectionFields.Length == 0)
                return _session.TrackEntity<T>(id, document, metadata, _disableEntitiesTracking);

            if (_projectionFields.Length == 1) // we only select a single field
            {
                var type = typeof(T);
                var typeInfo = type.GetTypeInfo();
                if (type == typeof(string) || typeInfo.IsValueType || typeInfo.IsEnum)
                {
                    var projectionField = _projectionFields[0];
                    T value;
                    return document.TryGet(projectionField, out value) == false
                        ? default(T)
                        : value;
                }
            }

            var result = (T)_session.Conventions.DeserializeEntityFromBlittable(typeof(T), document);

            if (string.IsNullOrEmpty(id) == false)
            {
                // we need to make an additional check, since it is possible that a value was explicitly stated
                // for the identity property, in which case we don't want to override it.
                object value;
                var identityProperty = _session.Conventions.GetIdentityProperty(typeof(T));
                if (identityProperty != null && (document.TryGetMember(identityProperty.Name, out value) == false || value == null))
                    _session.GenerateEntityIdOnTheClient.TrySetIdentity(result, id);
            }

            return result;
        }

        public bool DisableEntitiesTracking
        {
            get { return _disableEntitiesTracking; }
            set { _disableEntitiesTracking = value; }
        }

        public void EnsureIsAcceptableAndSaveResult(QueryResult result)
        {
            if (_waitForNonStaleResults && result.IsStale)
            {
                if (_sp.Elapsed > _timeout)
                {
                    _sp.Stop();
                    throw new TimeoutException(
                        string.Format("Waited for {0:#,#;;0}ms for the query to return non stale result.", _sp.ElapsedMilliseconds));
                }
            }

            _currentQueryResults = result;
            _currentQueryResults.EnsureSnapshot();

            if (_logger.IsInfoEnabled)
            {
                var isStale = result.IsStale ? "stale " : "";
                _logger.Info($"Query returned {result.Results.Items.Count()}/{result.TotalResults} {isStale}results");
            }
        }

        public IndexQuery IndexQuery
        {
            get { return _indexQuery; }
        }

        public string IndexName
        {
            get { return _indexName; }
        }
    }
}