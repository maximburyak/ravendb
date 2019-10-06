using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Util;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.Documents.PeriodicBackup.Retention;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Rachis;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.ServerWide
{
    public class ServerStoreBackupTask
    {
        private readonly ServerStore _serverStore;        
        private readonly DateTime _startTime;
        private readonly PeriodicBackup _periodicBackup;
        private readonly PeriodicBackupConfiguration _configuration;
        private readonly PeriodicBackupStatus _previousBackupStatus;        
        private readonly bool _backupToLocalFolder;
        private readonly long _operationId;
        private readonly PathSetting _tempBackupPath;
        private readonly Logger _logger;        
        public readonly OperationCancelToken TaskCancelToken;
        private readonly BackupResult _backupResult;        
        private readonly RetentionPolicyBaseParameters _retentionPolicyParameters;        
        private Action<IOperationProgress> _onProgress;        

        public ServerStoreBackupTask(
            ServerStore serverStore,            
            PeriodicBackup periodicBackup,            
            bool backupToLocalFolder,
            long operationId,
            PathSetting tempBackupPath,
            Logger logger)
        {
            _serverStore = serverStore;            
            _startTime = periodicBackup.StartTime;
            _periodicBackup = periodicBackup;
            _configuration = periodicBackup.Configuration;            
            _previousBackupStatus = periodicBackup.BackupStatus;            
            _backupToLocalFolder = backupToLocalFolder;
            _operationId = operationId;
            _tempBackupPath = tempBackupPath;
            _logger = logger;            

            TaskCancelToken = new OperationCancelToken(_serverStore.ServerShutdown);
            _backupResult = GenerateBackupResult();

            _retentionPolicyParameters = new RetentionPolicyBaseParameters
            {
                RetentionPolicy = _configuration.RetentionPolicy,
                DatabaseName = null,
                IsFullBackup = true,
                OnProgress = AddInfo,
                CancellationToken = TaskCancelToken.Token
            };
        }

        public IOperationResult RunPeriodicBackup(Action<IOperationProgress> onProgress)
        {
            _onProgress = onProgress;
            AddInfo($"Started task: '{_configuration.Name}'");

            var totalSw = Stopwatch.StartNew();
            var operationCanceled = false;

            var runningBackupStatus = _periodicBackup.RunningBackupStatus = new PeriodicBackupStatus
            {
                TaskId = _configuration.TaskId,
                BackupType = _configuration.BackupType,
                LastEtag = _previousBackupStatus.LastEtag,
                LastRaftIndex = _previousBackupStatus.LastRaftIndex,
                LastFullBackup = _previousBackupStatus.LastFullBackup,
                LastIncrementalBackup = _previousBackupStatus.LastIncrementalBackup,
                LastFullBackupInternal = _previousBackupStatus.LastFullBackupInternal,
                LastIncrementalBackupInternal = _previousBackupStatus.LastIncrementalBackupInternal,
                IsFull = true,
                LocalBackup = _previousBackupStatus.LocalBackup,
                LastOperationId = _previousBackupStatus.LastOperationId,
                FolderName = _previousBackupStatus.FolderName
            };

            try
            {
                if (runningBackupStatus.LocalBackup == null)
                    runningBackupStatus.LocalBackup = new LocalBackup();

                if (runningBackupStatus.LastRaftIndex == null)
                    runningBackupStatus.LastRaftIndex = new LastRaftIndex();

                if (_logger.IsInfoEnabled)
                {
                    var fullBackupText = "a " + (_configuration.BackupType == BackupType.Backup ? "full backup" : "snapshot");
                    _logger.Info($"Creating {fullBackupText}");
                }

                var localSettings = GetBackupConfigurationFromScript(_configuration.LocalSettings, x => JsonDeserializationServer.LocalSettings(x));

                GenerateFolderNameAndBackupDirectory(localSettings, out var now, out var folderName, out var backupDirectory);
                
                var isEncrypted = CheckIfEncrypted();
                var fileName = BackupTask.GetFileName(true, backupDirectory.FullPath, now, _configuration.BackupType, isEncrypted, out string backupFilePath);
                var lastRaftIndex = CreateLocalBackupOrSnapshot(runningBackupStatus, backupFilePath);

                runningBackupStatus.LocalBackup.BackupDirectory = _backupToLocalFolder ? backupDirectory.FullPath : null;
                runningBackupStatus.LocalBackup.TempFolderUsed = _backupToLocalFolder == false;
                runningBackupStatus.IsFull = true;

                try
                {
                    UploadToServer(backupFilePath, folderName, fileName);
                }
                finally
                {
                    runningBackupStatus.UploadToS3 = _backupResult.S3Backup;
                    runningBackupStatus.UploadToAzure = _backupResult.AzureBackup;
                    runningBackupStatus.UploadToGoogleCloud = _backupResult.GoogleCloudBackup;
                    runningBackupStatus.UploadToGlacier = _backupResult.GlacierBackup;
                    runningBackupStatus.UploadToFtp = _backupResult.FtpBackup;

                    // if user did not specify local folder we delete the temporary file
                    if (_backupToLocalFolder == false)
                    {
                        IOExtensions.DeleteFile(backupFilePath);
                    }
                }

                UpdateOperationId(runningBackupStatus);                
                runningBackupStatus.LastRaftIndex.LastEtag = lastRaftIndex;
                runningBackupStatus.FolderName = folderName;

                
                runningBackupStatus.LastFullBackup = _periodicBackup.StartTime;                

                totalSw.Stop();

                if (_logger.IsInfoEnabled)
                {                    
                    _logger.Info($"Successfully created a snapshot in {totalSw.ElapsedMilliseconds:#,#;;0} ms");
                }

                return _backupResult;
            }
            catch (OperationCanceledException)
            {
                operationCanceled = TaskCancelToken.Token.IsCancellationRequested &&
                                    _serverStore.ServerShutdown.IsCancellationRequested;
                throw;
            }
            catch (ObjectDisposedException)
            {
                // shutting down, probably
                operationCanceled = true;
                throw;
            }
            catch (Exception e)
            {
                const string message = "Error when performing periodic backup";

                runningBackupStatus.Error = new Error
                {
                    Exception = e.ToString(),
                    At = DateTime.UtcNow
                };

                if (_logger.IsOperationsEnabled)
                    _logger.Operations(message, e);

                _serverStore.NotificationCenter.Add(AlertRaised.Create(
                    "ServerStore",
                    $"ServerStore Periodic Backup task: '{_periodicBackup.Configuration.Name}'",
                    message,
                    AlertType.PeriodicBackup,
                    NotificationSeverity.Error,
                    details: new ExceptionDetails(e)));

                throw;
            }
            finally
            {
                if (operationCanceled == false)
                {
                    // whether we succeeded or not,
                    // we need to update the last backup time to avoid
                    // starting a new backup right after this one                    
                    runningBackupStatus.LastFullBackupInternal = _startTime;                    

                    runningBackupStatus.NodeTag = _serverStore.NodeTag;
                    runningBackupStatus.DurationInMs = totalSw.ElapsedMilliseconds;
                    runningBackupStatus.Version = ++_previousBackupStatus.Version;

                    _periodicBackup.BackupStatus = runningBackupStatus;

                    // save the backup status
                    WriteStatus(runningBackupStatus);
                }
            }
        }

        private T GetBackupConfigurationFromScript<T>(T backupSettings, Func<BlittableJsonReaderObject, T> deserializeSettingsFunc)
            where T : BackupSettings
        {
            if (backupSettings == null)
                return null;

            if (backupSettings.GetBackupConfigurationScript == null || backupSettings.Disabled)
                return backupSettings;

            if (string.IsNullOrEmpty(backupSettings.GetBackupConfigurationScript.Exec))
                return backupSettings;

            var command = backupSettings.GetBackupConfigurationScript.Exec;
            var arguments = backupSettings.GetBackupConfigurationScript.Arguments;

            var startInfo = new ProcessStartInfo
            {
                FileName = command,
                Arguments = arguments,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true
            };

            Process process;

            try
            {
                process = Process.Start(startInfo);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Unable to get backup configuration by executing {command} {arguments}. Failed to start process.", e);
            }

            using (var ms = new MemoryStream())
            {
                var readErrors = process.StandardError.ReadToEndAsync();
                var readStdOut = process.StandardOutput.BaseStream.CopyToAsync(ms);
                var timeoutInMs = backupSettings.GetBackupConfigurationScript.TimeoutInMs;

                string GetStdError()
                {
                    try
                    {
                        return readErrors.Result;
                    }
                    catch
                    {
                        return "Unable to get stdout";
                    }
                }

                try
                {
                    readStdOut.Wait(timeoutInMs);
                    readErrors.Wait(timeoutInMs);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Unable to get backup configuration by executing {command} {arguments}, waited for {timeoutInMs}ms but the process didn't exit. Stderr: {GetStdError()}", e);
                }

                if (process.WaitForExit(timeoutInMs) == false)
                {
                    process.Kill();

                    throw new InvalidOperationException($"Unable to get backup configuration by executing {command} {arguments}, waited for {timeoutInMs}ms but the process didn't exit. Stderr: {GetStdError()}");
                }

                if (process.ExitCode != 0)
                {
                    throw new InvalidOperationException($"Unable to get backup configuration by executing {command} {arguments}, the exit code was {process.ExitCode}. Stderr: {GetStdError()}");
                }

                using (_serverStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    ms.Position = 0;
                    var configuration = context.ReadForMemory(ms, "server-store-backup-configuration-from-script");
                    var result = deserializeSettingsFunc(configuration);
                    return result;
                }
            }
        }

        private bool CheckIfEncrypted()
        {
            
            if (_serverStore._env.Options.MasterKey != null &&
                _configuration.BackupEncryptionSettings?.EncryptionMode == EncryptionMode.UseDatabaseKey)
                return true;

            return _configuration.BackupEncryptionSettings != null &&
                   _configuration.BackupEncryptionSettings?.EncryptionMode != EncryptionMode.None;
        }        

        private void GenerateFolderNameAndBackupDirectory(LocalSettings localSettings, out string now, out string folderName, out PathSetting backupDirectory)
        {
            do
            {
                now = GetFormattedDate();
                folderName = $"{now}.ravendb_server_store-{_serverStore.NodeTag}-{_configuration.BackupType.ToString().ToLower()}";
                backupDirectory = _backupToLocalFolder ? new PathSetting(localSettings.FolderPath).Combine(folderName) : _tempBackupPath;
            } while (_backupToLocalFolder && DirectoryContainsBackupFiles(backupDirectory.FullPath, IsAnyBackupFile));

            if (Directory.Exists(backupDirectory.FullPath) == false)
                Directory.CreateDirectory(backupDirectory.FullPath);
        }

        private static string GetFormattedDate()
        {
            return DateTime.Now.ToString(BackupTask.DateTimeFormat, CultureInfo.InvariantCulture);
        }

        private BackupResult GenerateBackupResult()
        {
            return new BackupResult
            {
                SnapshotBackup =
                {
                    Skipped = false
                },
                S3Backup =
                {
                    // will be set before the actual upload if needed
                    Skipped = true
                },
                AzureBackup =
                {
                    Skipped = true
                },
                GoogleCloudBackup =
                {
                    Skipped = true
                },
                GlacierBackup =
                {
                    Skipped = true
                },
                FtpBackup =
                {
                    Skipped = true
                }
            };
        }

        public static bool DirectoryContainsBackupFiles(string fullPath, Func<string, bool> isBackupFile)
        {
            if (Directory.Exists(fullPath) == false)
                return false;

            var files = Directory.GetFiles(fullPath);
            if (files.Length == 0)
                return false;

            return files.Any(isBackupFile);
        }

        private static bool IsAnyBackupFile(string filePath)
        {
            if (RestorePointsBase.IsBackupOrSnapshot(filePath))
                return true;

            var extension = Path.GetExtension(filePath);
            return BackupTask.InProgressExtension.Equals(extension, StringComparison.OrdinalIgnoreCase);
        }      

        private static string GetFileNameFor(
            string backupExtension,
            string now,
            string backupFolder,
            out string backupFilePath,
            bool throwWhenFileExists = false)
        {
            var fileName = $"{now}{backupExtension}";
            backupFilePath = Path.Combine(backupFolder, fileName);

            if (File.Exists(backupFilePath))
            {
                if (throwWhenFileExists)
                    throw new InvalidOperationException($"File '{backupFilePath}' already exists!");

                while (true)
                {
                    fileName = $"{GetFormattedDate()}{backupExtension}";
                    backupFilePath = Path.Combine(backupFolder, fileName);

                    if (File.Exists(backupFilePath) == false)
                        break;
                }
            }

            return fileName;
        }

        private class InternalBackupResult
        {
            public long LastDocumentEtag { get; set; }
            public long LastRaftIndex { get; set; }
        }

        private long CreateLocalBackupOrSnapshot(
            PeriodicBackupStatus status, string backupFilePath)
        {

            long lastRaftIndex = _serverStore.LastRaftCommitIndex;
            using (status.LocalBackup.UpdateStats(true))
            {
                try
                {
                    // will rename the file after the backup is finished
                    var tempBackupFilePath = backupFilePath + BackupTask.InProgressExtension;

                    BackupTypeValidation();
               
                    AddInfo("Started a snapshot backup");
                                
                    var serverStoreSummary = _serverStore.GetServerStoreSummary();                    

                    var totalSw = Stopwatch.StartNew();
                    var sw = Stopwatch.StartNew();
                    _serverStore.FullBackupTo(tempBackupFilePath,
                        info =>
                        {
                            AddInfo(info.Message);

                            _backupResult.SnapshotBackup.ReadCount += info.FilesCount;
                            if (sw.ElapsedMilliseconds > 0 && info.FilesCount > 0)
                            {
                                AddInfo($"Backed up {_backupResult.SnapshotBackup.ReadCount} " +
                                        $"file{(_backupResult.SnapshotBackup.ReadCount > 1 ? "s" : string.Empty)}");
                                sw.Restart();
                            }
                        }, TaskCancelToken.Token);                    

                    AddInfo($"Backed up {_backupResult.SnapshotBackup.ReadCount} files, " +
                            $"took: {totalSw.ElapsedMilliseconds:#,#;;0}ms");                   

                    IOExtensions.RenameFile(tempBackupFilePath, backupFilePath);
                }
                catch (Exception e)
                {
                    status.LocalBackup.Exception = e.ToString();
                    throw;
                }
            }

            if (_backupToLocalFolder)
            {
                var localRetentionPolicy = new LocalRetentionPolicyRunner(_retentionPolicyParameters, _configuration.LocalSettings.FolderPath);
                localRetentionPolicy.Execute();
            }

            return lastRaftIndex;
        }

        private void BackupTypeValidation()
        {
            if (_serverStore._env.State.Options.MasterKey == null &&
                _configuration.BackupEncryptionSettings?.EncryptionMode == EncryptionMode.UseDatabaseKey)
                throw new InvalidOperationException("Can't use database key for backup encryption, the key doesn't exist");

            if (_configuration.BackupType == BackupType.Snapshot && 
                _configuration.BackupEncryptionSettings != null &&
                _configuration.BackupEncryptionSettings.EncryptionMode == EncryptionMode.UseProvidedKey)
                throw new InvalidOperationException("Can't snapshot encrypted database with different key");
        }

        private void AddInfo(string message)
        {
            lock (this)
            {
                _backupResult.AddInfo(message);
                _onProgress.Invoke(_backupResult.Progress);
            }
        }

        private void UploadToServer(string backupPath, string folderName, string fileName)
        {
            var s3Settings = GetBackupConfigurationFromScript(_configuration.S3Settings, x => JsonDeserializationServer.S3Settings(x));
            var glacierSettings = GetBackupConfigurationFromScript(_configuration.GlacierSettings, x => JsonDeserializationServer.GlacierSettings(x));
            var azureSettings = GetBackupConfigurationFromScript(_configuration.AzureSettings, x => JsonDeserializationServer.AzureSettings(x));
            var googleCloudSettings = GetBackupConfigurationFromScript(_configuration.GoogleCloudSettings, x => JsonDeserializationServer.GoogleCloudSettings(x));
            var ftpSettings = GetBackupConfigurationFromScript(_configuration.FtpSettings, x => JsonDeserializationServer.FtpSettings(x));

            TaskCancelToken.Token.ThrowIfCancellationRequested();

            var uploaderSettings = new BackupUploaderSettings
            {
                S3Settings = s3Settings,
                GlacierSettings = glacierSettings,
                AzureSettings = azureSettings,
                GoogleCloudSettings = googleCloudSettings,
                FtpSettings = ftpSettings,

                BackupPath = backupPath,
                FolderName = folderName,
                FileName = fileName,
                TaskName = _configuration.Name,
                BackupType = _configuration.BackupType
            };

            var backupUploader = new BackupUploader(uploaderSettings, _retentionPolicyParameters, _logger, _backupResult, _onProgress, TaskCancelToken);
            backupUploader.Execute();
        }

        private void UpdateOperationId(PeriodicBackupStatus runningBackupStatus)
        {
            runningBackupStatus.LastOperationId = _operationId;
            if (_previousBackupStatus.LastOperationId == null ||
                _previousBackupStatus.NodeTag != _serverStore.NodeTag)
                return;

            // dismiss the previous operation
            var id = $"{NotificationType.OperationChanged}/{_previousBackupStatus.LastOperationId.Value}";
            _serverStore.NotificationCenter.Dismiss(id);
        }

        private void WriteStatus(PeriodicBackupStatus status)
        {
            AddInfo("Saving backup status");

            try
            {
                var command = new Commands.PeriodicBackup.UpdateServerStoreBackupStatusCommand(RaftIdGenerator.NewId())
                {
                    PeriodicBackupStatus = status
                };

                var result = AsyncHelpers.RunSync(() => _serverStore.SendToLeaderAsync(command));

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Periodic backup status with task id {status.TaskId} was updated");

                AsyncHelpers.RunSync(() => _serverStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, result.Index));
            }
            catch (Exception e)
            {
                const string message = "Error saving the periodic backup status";

                if (_logger.IsOperationsEnabled)
                    _logger.Operations(message, e);
            }
        }
    }
}
