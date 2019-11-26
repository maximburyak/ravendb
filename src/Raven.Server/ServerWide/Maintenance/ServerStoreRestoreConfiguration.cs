using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents.Operations.Backups;

namespace Raven.Server.ServerWide.Maintenance
{
    public abstract class ServerStoreRestoreBackupConfigurationBase
    {
        public string DataDirectory { get; set; }

        public string EncryptionKey { get; set; }

        public bool DisableOngoingTasks { get; set; }

        public bool SkipIndexes { get; set; }

        protected abstract RestoreType Type { get; }

        public BackupEncryptionSettings BackupEncryptionSettings { get; set; }
    }

    public class ServerStoreRestoreBackupConfiguration : ServerStoreRestoreBackupConfigurationBase
    {
        public string BackupLocation { get; set; }

        protected override RestoreType Type => RestoreType.Local;
    }

    public class ServerStoreRestoreFromS3Configuration : ServerStoreRestoreBackupConfigurationBase
    {
        public S3Settings Settings { get; set; } = new S3Settings();
        protected override RestoreType Type => RestoreType.S3;
    }

    public class ServerStoreRestoreFromAzureConfiguration : ServerStoreRestoreBackupConfigurationBase
    {
        public AzureSettings Settings { get; set; } = new AzureSettings();

        protected override RestoreType Type => RestoreType.Azure;
    }

    public class ServerStoreRestoreFromGoogleCloudConfiguration : ServerStoreRestoreBackupConfigurationBase
    {
        public GoogleCloudSettings Settings { get; set; } = new GoogleCloudSettings();

        protected override RestoreType Type => RestoreType.GoogleCloud;
    }
}
