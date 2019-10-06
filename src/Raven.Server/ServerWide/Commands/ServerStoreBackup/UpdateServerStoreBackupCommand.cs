using System;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.PeriodicBackup
{
    public class UpdateServerStoreBackupCommand : UpdateValueCommand<PeriodicBackupConfiguration>
    {
        public PeriodicBackupConfiguration Configuration;

        public UpdateServerStoreBackupCommand()
        {
            // for deserialization
        }

        public UpdateServerStoreBackupCommand(PeriodicBackupConfiguration configuration, string uniqueRequestId)
        {
            Configuration = configuration;
        }

        public override object ValueToJson()
        {
            throw new NotImplementedException();
        }

        public override BlittableJsonReaderObject GetUpdatedValue(JsonOperationContext context, BlittableJsonReaderObject previousValue, long index)
        {
            throw new NotImplementedException();
        }
    }
}
