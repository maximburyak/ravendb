using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Maintenance;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Logging;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server
{
    public class ServerStoreBackup: ClusterTestBase
    {
        public ServerStoreBackup(ITestOutputHelper output) : base(output)
        {

        }
        [Fact]
        public async Task Backup()
        {
            string serverStorePath = null;
            string serverPath = null;
            string backupPath = null;

            using (var server = GetNewServer(new ServerCreationOptions
            {
                RunInMemory = false
            }))
            {
                serverStorePath = server.ServerStore._env.Options.BasePath.ToString();
                serverPath = server.Configuration.Core.DataDirectory.ToString();

                using (var newServerStore = GetDocumentStore(new Options
                {
                    Server = server,
                    CreateDatabase = true
                }))
                {
                    CompareExchangeResult<string> cmpXchgResult = newServerStore.Operations.Send(
                        new PutCompareExchangeValueOperation<string>("foo", "bar", 0));


                    var tasks = new ConcurrentSet<Task>();
                    var logger = LoggingSource.Instance.GetLogger<ServerStoreBackup>("BackupTaskName");
                    backupPath = NewDataPath(suffix: "BackupFolder");
                                        
                    var config = new PeriodicBackupConfiguration
                    {
                        LocalSettings = new LocalSettings
                        {
                            FolderPath = backupPath
                        },
                        BackupType = BackupType.Snapshot,
                        FullBackupFrequency = "* */1 * * *"
                    };

                    var tempPath = (server.ServerStore.Configuration.Storage.TempPath ?? server.ServerStore.Configuration.Core.DataDirectory).Combine("PeriodicBackupTemp");
                    Console.WriteLine(tempPath);

                    var backupTask = new ServerStoreBackupTask(server.ServerStore,
                        new Raven.Server.Documents.PeriodicBackup.PeriodicBackup(tasks)
                        {
                            Configuration = config,
                            BackupStatus = new PeriodicBackupStatus
                            {
                                TaskId = 444,
                                LocalBackup = new LocalBackup
                                {
                                    BackupDirectory = "ss"
                                }
                            }
                        },
                        backupToLocalFolder: true,
                        444,
                        tempPath,
                        logger);
                    backupTask.RunPeriodicBackup(x =>
                    {

                        using (var context = JsonOperationContext.ShortTermSingleUse())
                            Console.WriteLine(context.ReadObject(x.ToJson(), "Backup Status").ToString());

                    });

                    WaitForUserToContinueTheTest(newServerStore);
                    await DisposeServerAndWaitForFinishOfDisposalAsync(server);
                   
                }

                Directory.Delete(serverStorePath, true);

                var localConfiguration = new ServerStoreRestoreBackupConfiguration
                {
                    BackupLocation = Directory.GetDirectories(backupPath).FirstOrDefault(),
                    DataDirectory = serverPath
                };
                var restoreBackupTask = new ServerStoreRestoreFromLocal(localConfiguration);
                restoreBackupTask.Execute(x =>
                {

                }).Wait();

                using (var resurrectedServer = GetNewServer(new ServerCreationOptions()
                {
                    DeletePrevious = false,
                    RunInMemory = false,
                    PartialPath = serverPath
                }))
                {
                    using (var newServerStore = GetDocumentStore(new Options
                    {
                        Server = resurrectedServer,
                        CreateDatabase = true
                    }))
                    {
                        var cmpXchgResult = newServerStore.Operations.Send(
                       new GetCompareExchangeValueOperation<string>("foo"));
                        Console.WriteLine(cmpXchgResult.Value);

                    }
                }
            }
        }
        
    }
}
