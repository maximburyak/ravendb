﻿using System;
using System.Collections.Generic;

namespace Raven.Abstractions.TimeSeries
{
    public class TimeSeriesStorageDocument
    {
        /// <summary>
        /// The ID can be either the time series storage name ("TimeSeriesName") or the full document name ("Raven/TimeSereis/TimeSereisName").
        /// </summary>
        public string Id { get; set; }

        public Dictionary<string, string> Settings { get; set; }

        public Dictionary<string, string> SecuredSettings { get; set; } //preparation for air conditioner

        public bool Disabled { get; set; }

		public TimeSeriesStorageDocument()
        {
            Settings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
			SecuredSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }
    }
}