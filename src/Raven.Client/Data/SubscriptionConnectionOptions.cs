// -----------------------------------------------------------------------
//  <copyright file="SubscriptionBatchOptions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;
using Raven.Abstractions.Util;

namespace Raven.Abstractions.Data
{
    public class SubscriptionConnectionOptions
    {
        private static int connectionCounter;

        public SubscriptionConnectionOptions()
        {
            ConnectionId = Interlocked.Increment(ref connectionCounter) + "/" + Base62Util.Base62Random();
            BatchOptions = new SubscriptionBatchOptions();
            ClientAliveNotificationInterval = TimeSpan.FromMinutes(2).Ticks;
            TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(15).Ticks;
            PullingRequestTimeout = TimeSpan.FromMinutes(5).Ticks;
            Strategy = SubscriptionOpeningStrategy.OpenIfFree;
        }

        public string ConnectionId { get; private set; }

        public SubscriptionBatchOptions BatchOptions { get; set; }

        public long TimeToWaitBeforeConnectionRetry { get; set; }

        public long ClientAliveNotificationInterval { get; set; }

        public long PullingRequestTimeout { get; set; }

        public bool IgnoreSubscribersErrors { get; set; }

        public SubscriptionOpeningStrategy Strategy { get; set; }
    }

    public class SubscriptionBatchOptions
    {
        public SubscriptionBatchOptions()
        {
            MaxDocCount = 4096;
            AcknowledgmentTimeout = TimeSpan.FromMinutes(1).Ticks;
        }

        public int? MaxSize { get; set; }

        public int MaxDocCount { get; set; }

        public long AcknowledgmentTimeout { get; set; }
    }
}
