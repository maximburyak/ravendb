using System;
using System.Text.RegularExpressions;

namespace Raven.Client.ServerWide
{
    internal static class Helpers
    {
        private const string ValidDbNameChars = @"([A-Za-z0-9_\-\.]+)";

        public const string ValuesPrefix = "values/";
        public static string ClusterStateMachineValuesPrefix(string databaseName)
        {
            return $"{ValuesPrefix}{databaseName}/";
        }

        public static void AssertValidDatabaseName(string databaseName)
        {
            if (string.IsNullOrWhiteSpace(databaseName))
                throw new ArgumentNullException(nameof(databaseName));

            if (databaseName.Length > Constants.Documents.MaxDatabaseNameLength)
                throw new InvalidOperationException($"Name '{databaseName}' exceeds {Constants.Documents.MaxDatabaseNameLength} characters.");

            var result = Regex.Matches(databaseName, ValidDbNameChars);
            if (result.Count == 0 || result[0].Value != databaseName)
            {
                throw new InvalidOperationException(
                     "Database name can only contain A-Z, a-z, \"_\", \".\" or \"-\" but was: " + databaseName);
            }
        }
    }
}
