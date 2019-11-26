using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Aws;

namespace Raven.Server.ServerWide.Maintenance
{
    public class ServerStoreRestoreFromS3:ServerStoreRestoreTaskBase
    {
        private readonly RavenAwsS3Client _client;
        private readonly string _remoteFolderName;

        public ServerStoreRestoreFromS3(ServerStoreRestoreFromS3Configuration restoreFromConfiguration) : base(restoreFromConfiguration)
        {
            _client = new RavenAwsS3Client(restoreFromConfiguration.Settings);
            _remoteFolderName = restoreFromConfiguration.Settings.RemoteFolderName;
        }

        protected override async Task<Stream> GetStream(string path)
        {
            var blob = await _client.GetObjectAsync(path);
            return blob.Data;
        }

        protected override async Task<ZipArchive> GetZipArchiveForSnapshot(string path)
        {
            var blob = await _client.GetObjectAsync(path);
            return new ZipArchive(blob.Data, ZipArchiveMode.Read);
        }

        protected override Task<ZipArchive> GetZipArchiveForSnapshotCalc(string path)
        {
            return GetZipArchiveForSnapshot(path);
        }

        protected override async Task<List<string>> GetFilesForRestore()
        {
            var prefix = string.IsNullOrEmpty(_remoteFolderName) ? "" : _remoteFolderName.TrimEnd('/') + "/";
            var allObjects = await _client.ListAllObjectsAsync(prefix, string.Empty, false);
            return allObjects.Select(x => x.FullPath).ToList();
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
