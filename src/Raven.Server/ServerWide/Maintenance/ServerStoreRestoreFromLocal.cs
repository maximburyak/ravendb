using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;

namespace Raven.Server.ServerWide.Maintenance
{
    public class ServerStoreRestoreFromLocal:ServerStoreRestoreTaskBase
    {
        private readonly string _backupLocation;

        public ServerStoreRestoreFromLocal(ServerStoreRestoreBackupConfiguration restoreConfiguration) : base(restoreConfiguration)
        {
            if (string.IsNullOrWhiteSpace(restoreConfiguration.BackupLocation))
                throw new ArgumentException("Backup location can't be null or empty");

            if (Directory.Exists(restoreConfiguration.BackupLocation) == false)
                throw new ArgumentException($"Backup location doesn't exist, path: {_backupLocation}");

            _backupLocation = restoreConfiguration.BackupLocation;
        }

        protected override Task<Stream> GetStream(string path)
        {
            var stream = File.Open(path, FileMode.Open);
            return Task.FromResult<Stream>(stream);
        }

        protected override Task<ZipArchive> GetZipArchiveForSnapshot(string path)
        {
            return Task.FromResult(ZipFile.Open(path, ZipArchiveMode.Read, System.Text.Encoding.UTF8));
        }

        protected override Task<ZipArchive> GetZipArchiveForSnapshotCalc(string path)
        {
            return Task.FromResult(ZipFile.OpenRead(path));

        }

        protected override Task<List<string>> GetFilesForRestore()
        {
            return Task.FromResult(Directory.GetFiles(_backupLocation).ToList());
        }

        protected override string GetBackupPath(string fileName)
        {
            return fileName;
        }

        protected override string GetSmugglerBackupPath(string smugglerFile)
        {
            return Path.Combine(_backupLocation, smugglerFile);
        }

        protected override string GetBackupLocation()
        {
            return _backupLocation;
        }
    }
}
