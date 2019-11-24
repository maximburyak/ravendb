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
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.Platform;
using Voron.Impl.Backup;
using Voron.Util.Settings;

namespace Raven.Server.ServerWide.Maintenance
{
    public abstract class ServerStoreRestoreTaskBase
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<ServerStoreRestoreTaskBase>("ServerStore");
                
        protected readonly RestoreBackupConfigurationBase RestoreFromConfiguration;//protect for using in GetFilesForRestore()
        private readonly string _nodeTag;
        private readonly OperationCancelToken _operationCancelToken;
        private bool _hasEncryptionKey;
        private readonly bool _restoringToDefaultDataDirectory;
        private readonly ServerStore _serverStore;

        protected ServerStoreRestoreTaskBase(RestoreBackupConfigurationBase restoreFromConfiguration,
            string nodeTag,
            OperationCancelToken operationCancelToken, 
            ServerStore serverStore)
        {            
            RestoreFromConfiguration = restoreFromConfiguration;
            _nodeTag = nodeTag;
            _operationCancelToken = operationCancelToken;
            _serverStore = serverStore;
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
                var fileToRestore = (await GetFilesForRestore()).FirstOrDefault();

                if (fileToRestore == null || fileToRestore.Length == 0)
                {
                    throw new InvalidOperationException("No files were found to restore");
                }
                // todo: handle server store restore encryption

                if (onProgress == null)
                    onProgress = _ => { };

                Stopwatch sw = null;                

                var extension = Path.GetExtension(fileToRestore);

                onProgress.Invoke(result.Progress);

                sw = Stopwatch.StartNew();
                if (extension == Client.Constants.Documents.PeriodicBackup.EncryptedSnapshotExtension)
                {
                    _hasEncryptionKey = RestoreFromConfiguration.EncryptionKey != null ||
                                        RestoreFromConfiguration.BackupEncryptionSettings?.Key != null;
                }
                // restore the snapshot
                await SnapshotRestore(fileToRestore, onProgress, result);
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
                        var deleteResult = await _serverStore.DeleteDatabaseAsync(RestoreFromConfiguration.DatabaseName, 
                            true, 
                            new[] { _serverStore.NodeTag }, 
                            RaftIdGenerator.DontCareId);
                        await _serverStore.Cluster.WaitForIndexNotification(deleteResult.Index);
                    }
                }

                result.AddError($"Error occurred during restore of ServerStore. Exception: {e.Message}");
                onProgress.Invoke(result.Progress);
                throw;
            }
            finally
            {
                Dispose();
            }
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
                    var restoreDirectory = voronDataDirectory.Combine(directory);

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
    }

}
