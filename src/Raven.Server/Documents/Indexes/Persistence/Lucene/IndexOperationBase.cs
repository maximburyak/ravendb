﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using Query = Lucene.Net.Search.Query;
using Version = Lucene.Net.Util.Version;
using lucene = Lucene.Net;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public abstract class IndexOperationBase : IDisposable
    {
        private static readonly ConcurrentDictionary<Type, bool> NotForQuerying = new ConcurrentDictionary<Type, bool>();

        protected readonly string _indexName;

        protected readonly Logger _logger;
        internal Index _index;

        protected IndexOperationBase(Index index, Logger logger)
        {
            _index = index;
            _indexName = index.Name;
            _logger = logger;
        }

        protected static RavenPerFieldAnalyzerWrapper CreateAnalyzer(Func<lucene.Analysis.Analyzer> createDefaultAnalyzer, IndexDefinitionBase indexDefinition, bool forQuerying = false)
        {
            if (indexDefinition.IndexFields.ContainsKey(Constants.Documents.Indexing.Fields.AllFields))
                throw new InvalidOperationException($"Detected '{Constants.Documents.Indexing.Fields.AllFields}'. This field should not be present here, because inheritance is done elsewhere.");

            var analyzers = new Dictionary<Type, lucene.Analysis.Analyzer>();

            var hasDefaultFieldOptions = false;
            lucene.Analysis.Analyzer defaultAnalyzerToUse = null;
            lucene.Analysis.Analyzer defaultAnalyzer = null;
            if (indexDefinition is MapIndexDefinition mid)
            {
                if (mid.IndexDefinition.Fields.TryGetValue(Constants.Documents.Indexing.Fields.AllFields, out var value))
                {
                    hasDefaultFieldOptions = true;

                    switch (value.Indexing)
                    {
                        case FieldIndexing.Exact:
                            defaultAnalyzerToUse = GetOrCreateAnalyzer(typeof(lucene.Analysis.KeywordAnalyzer), CreateKeywordAnalyzer);
                            break;
                        case FieldIndexing.Search:
                            if (value.Analyzer != null)
                                defaultAnalyzerToUse = GetAnalyzer(Constants.Documents.Indexing.Fields.AllFields, value.Analyzer, analyzers, forQuerying);

                            if (defaultAnalyzerToUse == null)
                                defaultAnalyzerToUse = GetOrCreateAnalyzer(typeof(RavenStandardAnalyzer), CreateStandardAnalyzer);
                            break;
                        default:
                            // explicitly ignore all other values
                            break;
                    }
                }
            }

            if (defaultAnalyzerToUse == null)
            {
                defaultAnalyzerToUse = defaultAnalyzer = createDefaultAnalyzer();
                analyzers.Add(defaultAnalyzerToUse.GetType(), defaultAnalyzerToUse);
            }

            var perFieldAnalyzerWrapper = forQuerying == false && indexDefinition.HasDynamicFields
                ? new RavenPerFieldAnalyzerWrapper(
                        defaultAnalyzerToUse, 
                        () => GetOrCreateAnalyzer(typeof(RavenStandardAnalyzer), CreateStandardAnalyzer),
                        () => GetOrCreateAnalyzer(typeof(lucene.Analysis.KeywordAnalyzer), CreateKeywordAnalyzer))
                : new RavenPerFieldAnalyzerWrapper(defaultAnalyzerToUse);

            foreach (var field in indexDefinition.IndexFields)
            {
                var fieldName = field.Value.Name;

                switch (field.Value.Indexing)
                {
                    case FieldIndexing.Exact:
                        var keywordAnalyzer = GetOrCreateAnalyzer(typeof(lucene.Analysis.KeywordAnalyzer), CreateKeywordAnalyzer);

                        perFieldAnalyzerWrapper.AddAnalyzer(fieldName, keywordAnalyzer);
                        break;
                    case FieldIndexing.Search:
                        var analyzer = GetAnalyzer(fieldName, field.Value.Analyzer, analyzers, forQuerying);
                        if (analyzer != null)
                        {
                            perFieldAnalyzerWrapper.AddAnalyzer(fieldName, analyzer);
                            continue;
                        }
                        AddStandardAnalyzer(fieldName);
                        break;
                    case FieldIndexing.Default:
                        if (hasDefaultFieldOptions)
                        {
                            // if we have default field options then we need to take into account overrides for regular fields

                            if (defaultAnalyzer == null)
                                defaultAnalyzer = createDefaultAnalyzer();

                            perFieldAnalyzerWrapper.AddAnalyzer(fieldName, defaultAnalyzer);
                            continue;
                        }
                        break;
                }
            }

            return perFieldAnalyzerWrapper;

            void AddStandardAnalyzer(string fieldName)
            {
                var standardAnalyzer = GetOrCreateAnalyzer(typeof(RavenStandardAnalyzer), CreateStandardAnalyzer);

                perFieldAnalyzerWrapper.AddAnalyzer(fieldName, standardAnalyzer);
            }

            lucene.Analysis.Analyzer GetOrCreateAnalyzer(Type analyzerType, Func<lucene.Analysis.Analyzer> createAnalyzer)
            {
                if (analyzers.TryGetValue(analyzerType, out var analyzer) == false)
                    analyzers[analyzerType] = analyzer = createAnalyzer();

                return analyzer;
            }

            lucene.Analysis.KeywordAnalyzer CreateKeywordAnalyzer()
            {
                return new lucene.Analysis.KeywordAnalyzer();
            }

            RavenStandardAnalyzer CreateStandardAnalyzer()
            {
                return new RavenStandardAnalyzer(Version.LUCENE_29);
            }
        }

        public abstract void Dispose();

        private static lucene.Analysis.Analyzer GetAnalyzer(string name, string analyzer, Dictionary<Type, lucene.Analysis.Analyzer> analyzers, bool forQuerying)
        {
            if (string.IsNullOrWhiteSpace(analyzer))
                return null;

            var analyzerType = IndexingExtensions.GetAnalyzerType(name, analyzer);

            if (forQuerying)
            {
                var notForQuerying = NotForQuerying
                    .GetOrAdd(analyzerType, t => t.GetCustomAttributes<NotForQueryingAttribute>(false).Any());

                if (notForQuerying)
                    return null;
            }

            if (analyzers.TryGetValue(analyzerType, out var analyzerInstance) == false)
                analyzers[analyzerType] = analyzerInstance = IndexingExtensions.CreateAnalyzerInstance(name, analyzerType);

            return analyzerInstance;
        }

        protected Query GetLuceneQuery(DocumentsOperationContext context, QueryMetadata metadata, BlittableJsonReaderObject parameters, lucene.Analysis.Analyzer analyzer, QueryBuilderFactories factories)
        {
            return GetLuceneQuery(context, metadata, metadata.Query.Where, parameters, analyzer, factories);
        }

        protected Query GetLuceneQuery(DocumentsOperationContext context, QueryMetadata metadata, QueryExpression whereExpression, BlittableJsonReaderObject parameters, lucene.Analysis.Analyzer analyzer, QueryBuilderFactories factories)
        {
            Query documentQuery;

            if (metadata.Query.Where == null)
            {                
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Issuing query on index {_indexName} for all documents");

                
                if (metadata.OrderBy == null)                
                    documentQuery = new LimitedNumberOfMatchAllDocsQuery { MinPageSize = 1 };
                else
                    documentQuery = new lucene.Search.MatchAllDocsQuery();
            }
            else
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Issuing query on index {_indexName} for: {metadata.Query}");

                // RavenPerFieldAnalyzerWrapper searchAnalyzer = null;
                try
                {
                    //_persistence._a
                    //searchAnalyzer = parent.CreateAnalyzer(new LowerCaseKeywordAnalyzer(), toDispose, true);
                    //searchAnalyzer = parent.AnalyzerGenerators.Aggregate(searchAnalyzer, (currentAnalyzer, generator) =>
                    //{
                    //    Analyzer newAnalyzer = generator.GenerateAnalyzerForQuerying(parent.PublicName, query.Query, currentAnalyzer);
                    //    if (newAnalyzer != currentAnalyzer)
                    //    {
                    //        DisposeAnalyzerAndFriends(toDispose, currentAnalyzer);
                    //    }
                    //    return parent.CreateAnalyzer(newAnalyzer, toDispose, true);
                    //});

                    IDisposable releaseServerContext = null;
                    IDisposable closeServerTransaction = null;
                    TransactionOperationContext serverContext = null;

                    try
                    {
                        if (metadata.HasCmpXchg)
                        {
                            releaseServerContext = context.DocumentDatabase.ServerStore.ContextPool.AllocateOperationContext(out serverContext);
                            closeServerTransaction = serverContext.OpenReadTransaction();
                        }

                        using (closeServerTransaction)
                            documentQuery = QueryBuilder.BuildQuery(serverContext, context, metadata, whereExpression, _index.Definition, parameters, analyzer, factories);
                    }
                    finally
                    {
                        releaseServerContext?.Dispose();
                    }
                }
                finally
                {
                    //DisposeAnalyzerAndFriends(toDispose, searchAnalyzer);
                }
            }

            //var afterTriggers = ApplyIndexTriggers(documentQuery);

            return documentQuery;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static int GetPageSize(lucene.Search.IndexSearcher searcher, long pageSize)
        {
            if (pageSize >= searcher.MaxDoc)
                return searcher.MaxDoc;

            if (pageSize >= int.MaxValue)
                return int.MaxValue;

            return (int)pageSize;
        }
    }
}
