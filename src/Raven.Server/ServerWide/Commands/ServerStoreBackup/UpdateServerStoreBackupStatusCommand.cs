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

        }

        public override BlittableJsonReaderObject GetUpdatedValue(JsonOperationContext context, BlittableJsonReaderObject previousValue, long index)
        {
            throw new System.NotImplementedException();
        }

        public override object ValueToJson()
        {
            throw new System.NotImplementedException();
        }
    }
}
