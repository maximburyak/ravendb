using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.PeriodicBackup
{
    public class UpdateServerStoreBackupStatusCommand : UpdateValueCommand<PeriodicBackupStatus>
    {
        public PeriodicBackupStatus PeriodicBackupStatus;

        public UpdateServerStoreBackupStatusCommand()
        {

        }

        public UpdateServerStoreBackupStatusCommand(string Id)
        {
            UniqueRequestId = Id;
        }

        public override BlittableJsonReaderObject GetUpdatedValue(JsonOperationContext context, BlittableJsonReaderObject previousValue, long index)
        {
            return context.ReadObject(new DynamicJsonValue
            {
                ["TaskId"] =1,
                ["Name"]="ServerStoreBackup"
            },"foobar");
        }

        public override object ValueToJson()
        {
            return new DynamicJsonValue {
                ["TaskId"] = 1,
                ["Name"] = "ServerStoreBackup"
            };
            //throw new System.NotImplementedException();
        }
    }
}
