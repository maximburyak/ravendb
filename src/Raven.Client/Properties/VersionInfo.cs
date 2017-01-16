﻿using System;
using System.Linq;
using System.Reflection;

[assembly: Raven.Client.RavenVersion(Build = "40", CommitHash = "400f636", Version = "4.0", FullVersion = "4.0.0-custom-40")]

namespace Raven.Client
{
    [AttributeUsage(AttributeTargets.Assembly)]
    public class RavenVersionAttribute : Attribute
    {
        public string CommitHash { get; set; }
        public string Build { get; set; }
        public string Version { get; set; }
        public string FullVersion { get; set; }

        private static int? _buildVersion;

        private static RavenVersionAttribute _instance;

        public static RavenVersionAttribute Instance
        {
            get
            {
                if (_instance == null)
                {
                    _instance = (RavenVersionAttribute)
                        typeof(RavenVersionAttribute).GetTypeInfo()
                            .Assembly.GetCustomAttributes(typeof(RavenVersionAttribute))
                            .Single();
                }

                return _instance;
            }
        }

        public int BuildVersion
        {
            get
            {
                if (_buildVersion == null)
                {
                    int _;
                    if (int.TryParse(Build, out _) == false)
                    {
                        _buildVersion = 40;
                    }
                    else
                    {
                        _buildVersion = _;
                    }
                }

                return _buildVersion.Value;
            }
        }
    }
}
