using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Routing;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Logging;
using Voron.Impl.Backup;
using Voron.Util.Settings;

namespace Raven.Server.ServerWide
{
    public abstract class ServerStoreRestoreTaskBase
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<ServerStoreRestoreTaskBase>("Server");
                
        protected readonly RestoreBackupConfigurationBase RestoreFromConfiguration;//protect for using in GetFilesForRestore()
        private readonly string _nodeTag;
        private readonly OperationCancelToken _operationCancelToken;
        private bool _hasEncryptionKey;
        private readonly bool _restoringToDefaultDataDirectory;

        protected ServerStoreRestoreTaskBase(RestoreBackupConfigurationBase restoreFromConfiguration,
            string nodeTag,
            OperationCancelToken operationCancelToken)
        {            
            RestoreFromConfiguration = restoreFromConfiguration;
            _nodeTag = nodeTag;
            _operationCancelToken = operationCancelToken;

            if (string.IsNullOrWhiteSpace(RestoreFromConfiguration.DatabaseName))
                throw new ArgumentException("Database name can't be null or empty");
                        

            _hasEncryptionKey = string.IsNullOrWhiteSpace(RestoreFromConfiguration.EncryptionKey) == false;
            if (_hasEncryptionKey)
            {
                var key = Convert.FromBase64String(RestoreFromConfiguration.EncryptionKey);
                if (key.Length != 256 / 8)
                    throw new InvalidOperationException($"The size of the key must be 256 bits, but was {key.Length * 8} bits.");                                
            }

            var hasRestoreDataDirectory = string.IsNullOrWhiteSpace(RestoreFromConfiguration.DataDirectory) == false;
            if (hasRestoreDataDirectory &&
                HasFilesOrDirectories(RestoreFromConfiguration.DataDirectory))
                throw new ArgumentException("New data directory must be empty of any files or folders, " +
                                            $"path: {RestoreFromConfiguration.DataDirectory}");

            if (hasRestoreDataDirectory == false)
                RestoreFromConfiguration.DataDirectory = GetDataDirectory();

            _restoringToDefaultDataDirectory = IsDefaultDataDirectory(RestoreFromConfiguration.DataDirectory, RestoreFromConfiguration.DatabaseName);
        }

        protected abstract Task<Stream> GetStream(string path);

        protected abstract Task<ZipArchive> GetZipArchiveForSnapshot(string path);
        protected abstract Task<ZipArchive> GetZipArchiveForSnapshotCalc(string path);

        protected abstract Task<List<string>> GetFilesForRestore();

        protected abstract string GetBackupPath(string smugglerFile);
        protected abstract string GetSmugglerBackupPath(string smugglerFile);

        protected abstract string GetBackupLocation();

        public async Task<IOperationResult> Execute(Action<IOperationProgress> onProgress)
        {            
            var result = new RestoreResult
            {
                DataDirectory = RestoreFromConfiguration.DataDirectory
            };

            try
            {
                var filesToRestore = (await GetFilesForRestore()).FirstOrDefault();

                if (filesToRestore == null || filesToRestore.Length == 0)
                {
                    throw new InvalidOperationException("No files were found to restore");
                }
                // todo: handle server store restore encryption

                if (onProgress == null)
                    onProgress = _ => { };

                Stopwatch sw = null;
                var firstFile = filesToRestore[0];

                var extension = Path.GetExtension(firstFile);

                onProgress.Invoke(result.Progress);

                sw = Stopwatch.StartNew();
                if (extension == Client.Constants.Documents.PeriodicBackup.EncryptedSnapshotExtension)
                {
                    _hasEncryptionKey = RestoreFromConfiguration.EncryptionKey != null ||
                                        RestoreFromConfiguration.BackupEncryptionSettings?.Key != null;
                }
                // restore the snapshot
                await SnapshotRestore(firstFile, onProgress, result);

                

                result.SnapshotRestore.Processed = true;                                        

                result.AddInfo($"Successfully restored {result.SnapshotRestore.ReadCount} files during snapshot restore, took: {sw.ElapsedMilliseconds:#,#;;0}ms");
                onProgress.Invoke(result.Progress);
                    

                result.DatabaseRecord.Processed = true;
                result.Documents.Processed = true;
                result.RevisionDocuments.Processed = true;
                result.Conflicts.Processed = true;
                result.Indexes.Processed = true;
                result.Counters.Processed = true;
                result.Identities.Processed = true;
                result.CompareExchange.Processed = true;
                result.Subscriptions.Processed = true;
                onProgress.Invoke(result.Progress);
                    
                    
                // todo: set node to promotable, we'll probably need to load the serverstore in order to modify this value, or set some flag to settings.json

                return result;
                
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations("Failed to restore database", e);

                var alert = AlertRaised.Create(
                    RestoreFromConfiguration.DatabaseName,
                    "Failed to restore database",
                    $"Could not restore database named {RestoreFromConfiguration.DatabaseName}",
                    AlertType.RestoreError,
                    NotificationSeverity.Error,
                    details: new ExceptionDetails(e));
                _serverStore.NotificationCenter.Add(alert);

                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    bool databaseExists;
                    using (context.OpenReadTransaction())
                    {
                        databaseExists = _serverStore.Cluster.DatabaseExists(context, RestoreFromConfiguration.DatabaseName);
                    }

                    if (databaseExists == false)
                    {
                        // delete any files that we already created during the restore
                        IOExtensions.DeleteDirectory(RestoreFromConfiguration.DataDirectory);
                    }
                    else
                    {
                        var deleteResult = await _serverStore.DeleteDatabaseAsync(RestoreFromConfiguration.DatabaseName, true, new[] { _serverStore.NodeTag }, RaftIdGenerator.DontCareId);
                        await _serverStore.Cluster.WaitForIndexNotification(deleteResult.Index);
                    }
                }

                result.AddError($"Error occurred during restore of database {databaseName}. Exception: {e.Message}");
                onProgress.Invoke(result.Progress);
                throw;
            }
            finally
            {
                Dispose();
            }
        }

        private async Task<List<string>> GetOrderedFilesToRestore()
        {
            var files = await GetFilesForRestore();

            var orderedFiles = files
                .Where(RestorePointsBase.IsBackupOrSnapshot)
                .OrderBackups();

            if (orderedFiles == null)
                throw new ArgumentException("No files to restore from the backup location, " +
                                            $"path: {GetBackupLocation()}");

            if (string.IsNullOrWhiteSpace(RestoreFromConfiguration.LastFileNameToRestore))
                return orderedFiles.ToList();

            var filesToRestore = new List<string>();

            foreach (var file in orderedFiles)
            {
                filesToRestore.Add(file);
                if (file.Equals(RestoreFromConfiguration.LastFileNameToRestore, StringComparison.OrdinalIgnoreCase))
                    break;
            }

            return filesToRestore;
        }

        protected async Task<RestoreSettings> SnapshotRestore(string backupPath,
            Action<IOperationProgress> onProgress, RestoreResult restoreResult)
        {
            Debug.Assert(onProgress != null);

            RestoreSettings restoreSettings = null;

            var fullBackupPath = GetBackupPath(backupPath);
            using (var zip = await GetZipArchiveForSnapshot(fullBackupPath))
            {
                foreach (var zipEntries in zip.Entries.GroupBy(x => x.FullName.Substring(0, x.FullName.Length - x.Name.Length)))
                {
                    var directory = zipEntries.Key;

                    if (string.IsNullOrWhiteSpace(directory))
                    {
                        continue;
                    }

                    var voronDataDirectory = new VoronPathSetting(RestoreFromConfiguration.DataDirectory);
                    var restoreDirectory = directory.StartsWith(Client.Constants.Documents.PeriodicBackup.Folders.Documents, StringComparison.OrdinalIgnoreCase)
                        ? voronDataDirectory
                        : voronDataDirectory.Combine(directory);

                    BackupMethods.Full.Restore(
                        zipEntries,
                        restoreDirectory,
                        journalDir: null,
                        onProgress: message =>
                        {
                            restoreResult.AddInfo(message);
                            restoreResult.SnapshotRestore.ReadCount++;
                            onProgress.Invoke(restoreResult.Progress);
                        },
                        cancellationToken: _operationCancelToken.Token);
                }
            }

            if (restoreSettings == null)
                throw new InvalidDataException("Cannot restore the snapshot without the settings file!");

            return restoreSettings;
        }

        protected async Task SmugglerRestore(List<string> filesToRestore, Action<IOperationProgress> onProgress, string destinationPath, RestoreResult result)
        {
            Debug.Assert(onProgress != null);

            // the files are already ordered by name
            // take only the files that are relevant for smuggler restore

            if (filesToRestore.Count == 0)
                return;            
            

            // restore the smuggler backup
            var options = new DatabaseSmugglerOptionsServerSide
            {
                AuthorizationStatus = AuthorizationStatus.DatabaseAdmin,
                SkipRevisionCreation = true
            };

            options.OperateOnTypes |= DatabaseItemType.LegacyDocumentDeletions;
            options.OperateOnTypes |= DatabaseItemType.LegacyAttachments;
            options.OperateOnTypes |= DatabaseItemType.LegacyAttachmentDeletions;
#pragma warning disable 618
            options.OperateOnTypes |= DatabaseItemType.Counters;
#pragma warning restore 618

            var oldOperateOnTypes = Client.Documents.Smuggler.DatabaseSmuggler.ConfigureOptionsForIncrementalImport(options);
            
            for (var i = 0; i < filesToRestore.Count - 1; i++)
            {
                result.AddInfo($"Restoring file {(i + 1):#,#;;0}/{filesToRestore.Count:#,#;;0}");
                onProgress.Invoke(result.Progress);

                var filePath = GetBackupPath(filesToRestore[i]);
                await ImportSingleBackupFile(onProgress, result, filePath, destinationPath, options, isLastFile: false);
            }

            options.OperateOnTypes = oldOperateOnTypes;
            var lastFilePath = GetBackupPath(filesToRestore.Last());

            result.AddInfo($"Restoring file {filesToRestore.Count:#,#;;0}/{filesToRestore.Count:#,#;;0}");

            onProgress.Invoke(result.Progress);

            await ImportSingleBackupFile(database, onProgress, result, lastFilePath, context, destination, options, isLastFile: true,
                onIndexAction: indexAndType =>
                {
                    if (this.RestoreFromConfiguration.SkipIndexes)
                        return;

                    switch (indexAndType.Type)
                    {
                        case IndexType.AutoMap:
                        case IndexType.AutoMapReduce:
                            var autoIndexDefinition = (AutoIndexDefinitionBase)indexAndType.IndexDefinition;
                            databaseRecord.AutoIndexes[autoIndexDefinition.Name] =
                                PutAutoIndexCommand.GetAutoIndexDefinition(autoIndexDefinition, indexAndType.Type);
                            break;
                        case IndexType.Map:
                        case IndexType.MapReduce:
                        case IndexType.JavaScriptMap:
                        case IndexType.JavaScriptMapReduce:
                            var indexDefinition = (IndexDefinition)indexAndType.IndexDefinition;
                            databaseRecord.Indexes[indexDefinition.Name] = indexDefinition;
                            break;
                        case IndexType.None:
                        case IndexType.Faulty:
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                },
                onDatabaseRecordAction: smugglerDatabaseRecord =>
                {
                    databaseRecord.ConflictSolverConfig = smugglerDatabaseRecord.ConflictSolverConfig;
                    foreach (var setting in smugglerDatabaseRecord.Settings)
                    {
                        databaseRecord.Settings[setting.Key] = setting.Value;
                    }
                    databaseRecord.SqlEtls = smugglerDatabaseRecord.SqlEtls;
                    databaseRecord.RavenEtls = smugglerDatabaseRecord.RavenEtls;
                    databaseRecord.PeriodicBackups = smugglerDatabaseRecord.PeriodicBackups;
                    databaseRecord.ExternalReplications = smugglerDatabaseRecord.ExternalReplications;
                    databaseRecord.Sorters = smugglerDatabaseRecord.Sorters;
                    databaseRecord.SinkPullReplications = smugglerDatabaseRecord.SinkPullReplications;
                    databaseRecord.HubPullReplications = smugglerDatabaseRecord.HubPullReplications;
                    databaseRecord.Revisions = smugglerDatabaseRecord.Revisions;
                    databaseRecord.Expiration = smugglerDatabaseRecord.Expiration;
                    databaseRecord.RavenConnectionStrings = smugglerDatabaseRecord.RavenConnectionStrings;
                    databaseRecord.SqlConnectionStrings = smugglerDatabaseRecord.SqlConnectionStrings;
                    databaseRecord.Client = smugglerDatabaseRecord.Client;

                    // need to enable revisions before import
                    database.DocumentsStorage.RevisionsStorage.InitializeFromDatabaseRecord(smugglerDatabaseRecord);
                });
        }
        private bool IsDefaultDataDirectory(string dataDirectory, string databaseName)
        {
            var defaultDataDirectory = RavenConfiguration.GetDataDirectoryPath(
                _serverStore.Configuration.Core,
                databaseName,
                ResourceType.Database);

            return PlatformDetails.RunningOnPosix == false
                ? string.Equals(defaultDataDirectory, dataDirectory, StringComparison.OrdinalIgnoreCase)
                : string.Equals(defaultDataDirectory, dataDirectory, StringComparison.Ordinal);
        }

        private string GetDataDirectory()
        {
            var dataDirectory =
                RavenConfiguration.GetDataDirectoryPath(
                    _serverStore.Configuration.Core,
                    RestoreFromConfiguration.DatabaseName,
                    ResourceType.Database);

            var i = 0;
            while (HasFilesOrDirectories(dataDirectory))
                dataDirectory += $"-{++i}";

            return dataDirectory;
        }

        private void DisableOngoingTasksIfNeeded(DatabaseRecord databaseRecord)
        {
            if (RestoreFromConfiguration.DisableOngoingTasks == false)
                return;

            if (databaseRecord.ExternalReplications != null)
            {
                foreach (var task in databaseRecord.ExternalReplications)
                {
                    task.Disabled = true;
                }
            }

            if (databaseRecord.RavenEtls != null)
            {
                foreach (var task in databaseRecord.RavenEtls)
                {
                    task.Disabled = true;
                }
            }

            if (databaseRecord.SqlEtls != null)
            {
                foreach (var task in databaseRecord.SqlEtls)
                {
                    task.Disabled = true;
                }
            }

            if (databaseRecord.PeriodicBackups != null)
            {
                foreach (var task in databaseRecord.PeriodicBackups)
                {
                    task.Disabled = true;
                }
            }
        }

        private async Task ImportSingleBackupFile(
            Action<IOperationProgress> onProgress, RestoreResult restoreResult,
            string filePath, string destinationPath, DatabaseSmugglerOptionsServerSide options, bool isLastFile,
            Action<IndexDefinitionAndType> onIndexAction = null)
        {
            using (var fileStream = await GetStream(filePath))
            using (var inputStream = GetInputStream(fileStream))
            using (var gzipStream = new GZipStream(inputStream, CompressionMode.Decompress))
            using (var source = new StreamSource(gzipStream, context, database))
            {
                var smuggler = new Smuggler.Documents.DatabaseSmuggler(database, source, destination,
                    database.Time, options, result: restoreResult, onProgress: onProgress, token: _operationCancelToken.Token)
                {
                    OnIndexAction = onIndexAction,
                    OnDatabaseRecordAction = onDatabaseRecordAction
                };
                smuggler.Execute(ensureStepsProcessed: false, isLastFile);
            }
        }

        private Stream GetInputStream(Stream fileStream)
        {
            return RestoreFromConfiguration.BackupEncryptionSettings?.Key != null ? new DecryptingXChaCha20Oly1305Stream(fileStream, Convert.FromBase64String(RestoreFromConfiguration.BackupEncryptionSettings.Key)) : fileStream;
        }

        protected bool HasFilesOrDirectories(string location)
        {
            if (Directory.Exists(location) == false)
                return false;

            return Directory.GetFiles(location).Length > 0 ||
                   Directory.GetDirectories(location).Length > 0;
        }

        protected virtual void Dispose()
        {
            _operationCancelToken.Dispose();
        }

        public async Task<long> CalculateBackupSizeInBytes()
        {
            var zipPath = GetBackupLocation();
            zipPath = Path.Combine(zipPath, RestoreFromConfiguration.LastFileNameToRestore);
            using (var zip = await GetZipArchiveForSnapshotCalc(zipPath))
                return zip.Entries.Sum(entry => entry.Length);
        }
    }

}
