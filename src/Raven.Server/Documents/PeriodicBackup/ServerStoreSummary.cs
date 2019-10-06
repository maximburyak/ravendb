using System;
using System.Collections.Generic;
using System.Text;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class ServerStoreSummary
    {
        public long LastRaftCommitIndex { get; internal set; }
        public List<string> DatabaseNames { get; internal set; }
        public long CompareExchangeValuesCount { get; internal set; }
        public long CompareExchangeTombstonesCount { get; internal set; }
        public long IdentitiesCount { get; internal set; }
        public long ClusterStateMachineValuesCount { get; internal set; }
    }
}
