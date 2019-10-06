using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.ServerWide;
using Sparrow.Collections;
using Sparrow.Logging;
using Xunit;

namespace SlowTests.Server
{
    public class ServerStoreBackup:RavenTestBase
    {
        [Fact]
        public void Backup()
        {
            using (var store = GetDocumentStore())
            {
                var server = Server;
                var tasks = new ConcurrentSet<Task>();
                var logger = LoggingSource.Instance.GetLogger<ServerStoreBackup>("BackupTaskName");
                var backupPath = NewDataPath(suffix: "BackupFolder");

                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    BackupType=BackupType.Snapshot,
                    FullBackupFrequency = "* */1 * * *"                    
                };

                var backupTask  = new ServerStoreBackupTask(server.ServerStore,
                    new Raven.Server.Documents.PeriodicBackup.PeriodicBackup(tasks)
                    {
                        Configuration = config,
                        BackupStatus = new PeriodicBackupStatus
                        {
                            TaskId=444
                        }
                    },
                    backupToLocalFolder: true,
                    444,
                    (server.ServerStore.Configuration.Storage.TempPath ?? server.ServerStore.Configuration.Core.DataDirectory).Combine("PeriodicBackupTemp"),
                    logger);
                backupTask.RunPeriodicBackup(x => Console.WriteLine(x.ToString()));
            }
        }
    }
}
