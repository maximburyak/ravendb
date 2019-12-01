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
using Raven.Server.Config.Settings;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Utils;
using Voron.Impl.Backup;
using Voron.Util.Settings;

namespace Raven.Server.ServerWide.Maintenance
{
    public abstract class ServerStoreRestoreTaskBase
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<ServerStoreRestoreTaskBase>("ServerStore");
                
        protected readonly ServerStoreRestoreBackupConfigurationBase RestoreFromConfiguration;//protect for using in GetFilesForRestore()                
        private bool _hasEncryptionKey;
        private readonly bool _restoringToDefaultDataDirectory;        

        //RavenConfiguration.CreateForServer(null, settingsPath)

        protected ServerStoreRestoreTaskBase(ServerStoreRestoreBackupConfigurationBase restoreFromConfiguration)
        {            
            RestoreFromConfiguration = restoreFromConfiguration;

            _hasEncryptionKey = string.IsNullOrWhiteSpace(RestoreFromConfiguration.EncryptionKey) == false;
            if (_hasEncryptionKey)
            {
                var key = Convert.FromBase64String(RestoreFromConfiguration.EncryptionKey);
                if (key.Length != 256 / 8)
                    throw new InvalidOperationException($"The size of the key must be 256 bits, but was {key.Length * 8} bits.");                                
            }

            if (string.IsNullOrWhiteSpace(RestoreFromConfiguration.DataDirectory))
            {
                throw new ArgumentException("DataDirectory is a mandatory field of the ServerStore restore configuration");
            }

            if (HasFilesOrDirectories(RestoreFromConfiguration.DataDirectory))
                throw new ArgumentException("New data directory must be empty of any files or folders, " +
                                            $"path: {RestoreFromConfiguration.DataDirectory}");
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
                var fileToRestore = (await GetOrderedFilesToRestore()).FirstOrDefault();

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

                // delete any files that we already created during the restore
                IOExtensions.DeleteDirectory(RestoreFromConfiguration.DataDirectory);

                result.AddError($"Error occurred during restore of ServerStore. Exception: {e.Message}");
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

        public async Task<long> CalculateBackupSizeInBytes()
        {
            var zipPath = GetBackupLocation();
            zipPath = Path.Combine(zipPath, RestoreFromConfiguration.LastFileNameToRestore);
            using (var zip = await GetZipArchiveForSnapshotCalc(zipPath))
                return zip.Entries.Sum(entry => entry.Length);
        }

        private async Task ValidateFreeSpace()
        {
            long backupSizeInBytes;

            try
            {
                backupSizeInBytes = await CalculateBackupSizeInBytes();
            }
            catch (Exception e)
            {
                if (e is InvalidDataException)
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations($"Restore database from snapshot operation failed. Invalid snapshot file {RestoreFromConfiguration.LastFileNameToRestore}", e);
                    throw new InvalidDataException($"Invalid snapshot file {RestoreFromConfiguration.LastFileNameToRestore} ", e);
                }

                if (e is FileNotFoundException)
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations($"Restore database from snapshot operation failed. Could not find file {RestoreFromConfiguration.LastFileNameToRestore}", e);
                    throw new FileNotFoundException($"Could not find file {RestoreFromConfiguration.LastFileNameToRestore} ", e);
                }

                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"Restore database from snapshot operation failed. Error reading snapshot file {RestoreFromConfiguration.LastFileNameToRestore}", e);

                throw new IOException($"Error reading snapshot file {RestoreFromConfiguration.LastFileNameToRestore}. Please provide a valid snapshot file.", e);
            }

            var destinationPath = RestoreFromConfiguration.DataDirectory;

            var drivesInfo = PlatformDetails.RunningOnPosix ? DriveInfo.GetDrives() : null;
            var destinationDirInfo = DiskSpaceChecker.GetDriveInfo(destinationPath, drivesInfo, out _);
            var destinationDriveInfo = DiskSpaceChecker.GetDiskSpaceInfo(destinationDirInfo.DriveName);

            if (destinationDriveInfo == null)
                throw new ArgumentException($"Provided path starts with an invalid drive name. Please use a proper path. Drive name provided: {destinationDirInfo.DriveName}.");

            var desiredFreeSpace = Sparrow.Size.Min(new Sparrow.Size(512, SizeUnit.Megabytes), destinationDriveInfo.TotalSize * 0.01) + new Sparrow.Size(backupSizeInBytes, SizeUnit.Bytes);

            if (destinationDriveInfo.TotalFreeSpace < desiredFreeSpace)
                throw new ArgumentException($"No enough free space to restore a backup. Required space {desiredFreeSpace}, available space: {destinationDriveInfo.TotalFreeSpace}");
        }


        protected async Task SnapshotRestore(string backupPath,
            Action<IOperationProgress> onProgress, RestoreResult restoreResult)
        {
            Debug.Assert(onProgress != null);

            var fullBackupPath = GetBackupPath(backupPath);
            using (var zip = await GetZipArchiveForSnapshot(fullBackupPath))
            {
                var grouptedEntries = zip.Entries.GroupBy(x => x.FullName.Substring(0, x.FullName.Length - x.Name.Length)).ToArray();

                if (grouptedEntries.Length != 1)
                {
                    throw new ArgumentOutOfRangeException($"Number of entries in ServerStore backup archive must 1, instead it was {zip.Entries.Count}");
                }

                var voronDataDirectory = new VoronPathSetting(RestoreFromConfiguration.DataDirectory);
                var restoreDirectory = voronDataDirectory.Combine("System");

                BackupMethods.Full.Restore(
                    grouptedEntries[0],
                    restoreDirectory,
                    journalDir: null,
                    onProgress: message =>
                    {
                        restoreResult.AddInfo(message);
                        restoreResult.SnapshotRestore.ReadCount++;
                        onProgress.Invoke(restoreResult.Progress);
                    });
            }
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
        }
    }

}
