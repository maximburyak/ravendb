using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.GoogleCloud;

namespace Raven.Server.ServerWide.Maintenance
{
    public class ServerStoreRestoreFromGoogleCloud : ServerStoreRestoreTaskBase
    {
        private readonly RavenGoogleCloudClient _client;
        private readonly string _remoteFolderName;

        public ServerStoreRestoreFromGoogleCloud(ServerStore serverStore, RestoreFromGoogleCloudConfiguration restoreFromConfiguration) : base(serverStore, restoreFromConfiguration)
        {
            _client = new RavenGoogleCloudClient(restoreFromConfiguration.Settings);
            _remoteFolderName = restoreFromConfiguration.Settings.RemoteFolderName;
        }

        protected override Task<Stream> GetStream(string path)
        {
            return Task.FromResult(_client.DownloadObject(path));
        }

        protected override Task<ZipArchive> GetZipArchiveForSnapshot(string path)
        {
            return Task.FromResult(new ZipArchive(_client.DownloadObject(path), ZipArchiveMode.Read));
        }

        protected override Task<ZipArchive> GetZipArchiveForSnapshotCalc(string path)
        {
            return GetZipArchiveForSnapshot(path);
        }

        protected override async Task<List<string>> GetFilesForRestore()
        {
            var prefix = string.IsNullOrEmpty(_remoteFolderName) ? "" : _remoteFolderName.TrimEnd('/');
            var allObjects = await _client.ListObjectsAsync(prefix, delimiter: null);
            var result = new List<string>();
            foreach (var obj in allObjects)
            {
                result.Add(obj.Name);
            }

            return result;
        }

        protected override string GetBackupPath(string fileName)
        {
            return fileName;
        }

        protected override string GetSmugglerBackupPath(string smugglerFile)
        {
            return smugglerFile;
        }

        protected override string GetBackupLocation()
        {
            return _remoteFolderName;
        }

        protected override void Dispose()
        {
            using (_client)
            {
                base.Dispose();
            }
        }
    }
}
