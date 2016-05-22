﻿using System;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public class NotificationsClientConnection : IDisposable
    {
        private static long _counter = 0;

        private readonly WebSocket _webSocket;
        private readonly DocumentDatabase _documentDatabase;
        private readonly AsyncQueue<DynamicJsonValue> _sendQueue = new AsyncQueue<DynamicJsonValue>();
        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private readonly DateTime _startedAt;

        private readonly ConcurrentSet<string> _matchingIndexes =
            new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<string> _matchingDocuments =
            new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<string> _matchingDocumentPrefixes =
            new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<string> _matchingDocumentsInCollection =
            new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<string> _matchingDocumentsOfType =
            new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<string> _matchingBulkInserts =
            new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        private int watchAllDocuments;
        private int watchAllIndexes;
        private int watchAllTransformers;
        private int watchAllReplicationConflicts;
        private int watchAllDataSubscriptions;

        public NotificationsClientConnection(WebSocket webSocket, DocumentDatabase documentDatabase)
        {
            _webSocket = webSocket;
            _documentDatabase = documentDatabase;
            _startedAt = SystemTime.UtcNow;
        }

        public long Id = Interlocked.Increment(ref _counter);

        public TimeSpan Age => SystemTime.UtcNow - _startedAt;

        public void WatchDocument(string docId)
        {
            _matchingDocuments.TryAdd(docId);
        }

        public void UnwatchDocument(string name)
        {
            _matchingDocuments.TryRemove(name);
        }

        public void WatchAllDocuments()
        {
            Interlocked.Increment(ref watchAllDocuments);
        }

        public void UnwatchAllDocuments()
        {
            Interlocked.Decrement(ref watchAllDocuments);
        }

        public void WatchDocumentPrefix(string name)
        {
            _matchingDocumentPrefixes.TryAdd(name);
        }

        public void UnwatchDocumentPrefix(string name)
        {
            _matchingDocumentPrefixes.TryRemove(name);
        }

        public void WatchDocumentInCollection(string name)
        {
            _matchingDocumentsInCollection.TryAdd(name);
        }

        public void UnwatchDocumentInCollection(string name)
        {
            _matchingDocumentsInCollection.TryRemove(name);
        }

        public void WatchDocumentOfType(string name)
        {
            _matchingDocumentsOfType.TryAdd(name);
        }

        public void UnwatchDocumentOfType(string name)
        {
            _matchingDocumentsOfType.TryRemove(name);
        }

        public void SendDocumentChanges(DocumentChangeNotification notification)
        {
            if (watchAllDocuments > 0)
            {
                Send(notification);
                return;
            }

            if (notification.Key != null && _matchingDocuments.Contains(notification.Key))
            {
                Send(notification);
                return;
            }

            var hasPrefix = notification.Key != null && _matchingDocumentPrefixes
                .Any(x => notification.Key.StartsWith(x, StringComparison.OrdinalIgnoreCase));
            if (hasPrefix)
            {
                Send(notification);
                return;
            }

            var hasCollection = notification.CollectionName != null && _matchingDocumentsInCollection
                .Any(x => string.Equals(x, notification.CollectionName, StringComparison.OrdinalIgnoreCase));
            if (hasCollection)
            {
                Send(notification);
                return;
            }

            var hasType = notification.TypeName != null && _matchingDocumentsOfType
                .Any(x => string.Equals(x, notification.TypeName, StringComparison.OrdinalIgnoreCase));
            if (hasType)
            {
                Send(notification);
                return;
            }

            if (notification.Key == null && notification.CollectionName == null && notification.TypeName == null)
            {
                Send(notification);
            }
        }

        private void Send(DocumentChangeNotification notification)
        {
            var value = new DynamicJsonValue
            {
                ["Type"] = "DocumentChangeNotification",
                ["Value"] = new DynamicJsonValue
                {
                    ["Type"] = (int)notification.Type,
                    ["Key"] = notification.Key,
                    ["CollectionName"] = notification.CollectionName,
                    ["TypeName"] = notification.TypeName,
                    ["Etag"] = notification.Etag,
                },
            };

            if (_disposeToken.IsCancellationRequested == false)
                _sendQueue.Enqueue(value);
        }

        public async Task StartSendingNotifications()
        {
            JsonOperationContext context;
            using (_documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                using (var ms = new MemoryStream())
                {
                    while (true)
                    {
                        if (_disposeToken.IsCancellationRequested)
                            break;

                        ms.SetLength(0);
                        using (var writer = new BlittableJsonTextWriter(context, ms))
                        {
                            do
                            {
                                var value = await _sendQueue.DequeueAsync();
                                if (_disposeToken.IsCancellationRequested)
                                    break;

                                context.Write(writer, value);
                                writer.WriteNewLine();

                                if (ms.Length > 16*1024)
                                    break;
                            } while (_sendQueue.Count > 0);
                        }

                        ArraySegment<byte> bytes;
                        ms.TryGetBuffer(out bytes);
                        await _webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, _disposeToken.Token);
                    }
                }
            }
        }

        public void Dispose()
        {
            _disposeToken.Cancel();
            _sendQueue.Dispose();
        }

        public void Confirm(int commandId)
        {
            _sendQueue.Enqueue(new DynamicJsonValue
            {
                ["CommandId"] = commandId,
                ["Type"] = "Confirm"
            });
        }

        public void HandleCommand(string command, string commandParameter)
        {
            /* if (Match(command, "watch-index"))
             {
                 WatchIndex(commandParameter);
             }
             else if (Match(command, "unwatch-index"))
             {
                 UnwatchIndex(commandParameter);
             }
             else if (Match(command, "watch-indexes"))
             {
                 WatchAllIndexes();
             }
             else if (Match(command, "unwatch-indexes"))
             {
                 UnwatchAllIndexes();
             }
             else if (Match(command, "watch-transformers"))
             {
                 WatchTransformers();
             }
             else if (Match(command, "unwatch-transformers"))
             {
                 UnwatchTransformers();
             }
             else*/
            if (Match(command, "watch-doc"))
            {
                WatchDocument(commandParameter);
            }
            else if (Match(command, "unwatch-doc"))
            {
                UnwatchDocument(commandParameter);
            }
            else if (Match(command, "watch-docs"))
            {
                WatchAllDocuments();
            }
            else if (Match(command, "unwatch-docs"))
            {
                UnwatchAllDocuments();
            }
            else if (Match(command, "watch-prefix"))
            {
                WatchDocumentPrefix(commandParameter);
            }
            else if (Equals(command, "unwatch-prefix"))
            {
                UnwatchDocumentPrefix(commandParameter);
            }
            else if (Match(command, "watch-collection"))
            {
                WatchDocumentInCollection(commandParameter);
            }
            else if (Equals(command, "unwatch-collection"))
            {
                UnwatchDocumentInCollection(commandParameter);
            }
            else if (Match(command, "watch-type"))
            {
                WatchDocumentOfType(commandParameter);
            }
            else if (Equals(command, "unwatch-type"))
            {
                UnwatchDocumentOfType(commandParameter);
            }
            /*else if (Match(command, "watch-replication-conflicts"))
            {
                WatchAllReplicationConflicts();
            }
            else if (Match(command, "unwatch-replication-conflicts"))
            {
                UnwatchAllReplicationConflicts();
            }*/

            // todo: implement bulk operations watch
            else if (Match(command, "watch-bulk-operation"))
            {
                WatchAllDocuments();//WatchBulkInsert(commandParameter);
            }
            else if (Match(command, "unwatch-bulk-operation"))
            {
                WatchAllDocuments();//UnwatchBulkInsert(commandParameter);
            }/*
            else if (Match(command, "watch-data-subscriptions"))
            {
                WatchAllDataSubscriptions();
            }
            else if (Match(command, "unwatch-data-subscriptions"))
            {
                UnwatchAllDataSubscriptions();
            }
            else if (Match(command, "watch-data-subscription"))
            {
                WatchDataSubscription(long.Parse(commandParameter));
            }
            else if (Match(command, "unwatch-data-subscription"))
            {
                UnwatchDataSubscription(long.Parse(commandParameter));
            }*/
            else
            {
                throw new ArgumentOutOfRangeException(nameof(command), "Command argument is not valid");
            }
        }

        protected static bool Match(string x, string y)
        {
            return string.Equals(x, y, StringComparison.OrdinalIgnoreCase);
        }

        public DynamicJsonValue GetDebugInfo()
        {
            return new DynamicJsonValue
            {
                ["Id"] = Id,
                ["State"] = _webSocket.State.ToString(),
                ["CloseStatus"] = _webSocket.CloseStatus,
                ["CloseStatusDescription"] = _webSocket.CloseStatusDescription,
                ["SubProtocol"] = _webSocket.SubProtocol,
                ["Age"] = Age,
                ["WatchAllDocuments"] = watchAllDocuments > 0,
                ["WatchAllIndexes"] = watchAllIndexes > 0,
                ["WatchAllTransformers"] = watchAllTransformers > 0,
                /*["WatchConfig"] = _watchConfig > 0,
                ["WatchConflicts"] = _watchConflicts > 0,
                ["WatchSync"] = _watchSync > 0,*/
                ["WatchDocumentPrefixes"] = _matchingDocumentPrefixes.ToArray(),
                ["WatchDocumentsInCollection"] = _matchingDocumentsInCollection.ToArray(),
                ["WatchIndexes"] = _matchingIndexes.ToArray(),
                ["WatchDocuments"] = _matchingDocuments.ToArray(),
            };
        }
    }
}