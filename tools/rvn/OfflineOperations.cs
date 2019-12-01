﻿using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Util;
using Raven.Server.Config.Categories;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Maintenance;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Voron;
using Voron.Impl.Compaction;

namespace rvn
{
    internal class OfflineOperations
    {
        private const string SecretKeyEncrypted = "secret.key.encrypted";
        
        public static string GetKey(string srcDir)
        {
            var existingKeyBase64PlainText = RecoverServerStoreKey(srcDir);

            return $"GetKey: {Path.Combine(srcDir, SecretKeyEncrypted)} ***PLAIN TEXT*** key is: {existingKeyBase64PlainText}";
        }

        public static string PutKey(string destDir, string plainTextBase64KeyToPut)
        {
            var secret = Convert.FromBase64String(plainTextBase64KeyToPut);
            var protect = new SecretProtection(new SecurityConfiguration()).Protect(secret);

            using (var f = File.OpenWrite(Path.Combine(destDir, SecretKeyEncrypted)))
            {
                f.Write(protect, 0, protect.Length);
                f.Flush();
            }

            return $"PutKey: {Path.Combine(destDir, SecretKeyEncrypted)} Created Successfully";
        }

        public static string InitKeys()
        {
            return "InitKeys is not implemented";
        }

        public static string Encrypt(string srcDir)
        {
            var masterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());
            var dstDir = Path.Combine(Path.GetDirectoryName(srcDir), "Temp.Encryption");

            var srcOptions = StorageEnvironmentOptions.ForPath(srcDir);
            var dstOptions = StorageEnvironmentOptions.ForPath(dstDir);

            dstOptions.MasterKey = masterKey;

            var protect = new SecretProtection(new SecurityConfiguration()).Protect(masterKey);

            StorageCompaction.Execute(srcOptions, (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)dstOptions);

            using (var f = File.OpenWrite(Path.Combine(dstDir, SecretKeyEncrypted)))
            {
                f.Write(protect, 0, protect.Length);
                f.Flush();
            }

            IOExtensions.DeleteDirectory(srcDir);
            Directory.Move(dstDir, srcDir);

            return $"Encrypt: {Path.Combine(dstDir, SecretKeyEncrypted)} Created Successfully";
        }

        public static string Decrypt(string srcDir)
        {
            var dstDir = Path.Combine(Path.GetDirectoryName(srcDir), "Temp.Decryption");
            var bytes = File.ReadAllBytes(Path.Combine(srcDir, SecretKeyEncrypted));

            var srcOptions = StorageEnvironmentOptions.ForPath(srcDir);
            var dstOptions = StorageEnvironmentOptions.ForPath(dstDir);

            srcOptions.MasterKey = new SecretProtection(new SecurityConfiguration()).Unprotect(bytes);

            StorageCompaction.Execute(srcOptions, (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)dstOptions);

            IOExtensions.DeleteDirectory(srcDir);
            Directory.Move(dstDir, srcDir);

            return "Decrypt: Completed Successfully";
        }

        public static string RestorServerStore(BlittableJsonReaderObject restoreConfiguration)
        {

            RestoreType restoreType;
            if (restoreConfiguration.TryGet("Type", out string typeAsString))
            {
                if (Enum.TryParse(typeAsString, out restoreType) == false)
                    throw new ArgumentException($"{typeAsString} is unknown backup type.");
            }
            else
            {
                restoreType = RestoreType.Local;
            }

            ServerStoreRestoreTaskBase restoreBackupTask;
            switch (restoreType)
            {
                case RestoreType.Local:
                    var localConfiguration = JsonDeserializationCluster.ServerStoreRestoreBackupConfiguration(restoreConfiguration);
                    restoreBackupTask = new ServerStoreRestoreFromLocal(localConfiguration);                    
                    break;

                case RestoreType.S3:
                    var s3Configuration = JsonDeserializationCluster.ServerStoreRestoreS3BackupConfiguration(restoreConfiguration);
                    restoreBackupTask = new ServerStoreRestoreFromS3(s3Configuration);                    

                    break;
                case RestoreType.Azure:
                    var azureConfiguration = JsonDeserializationCluster.ServerStoreRestoreAzureBackupConfiguration(restoreConfiguration);
                    restoreBackupTask = new ServerStoreRestoreFromAzure(azureConfiguration);
                    break;
                case RestoreType.GoogleCloud:
                    var googlCloudConfiguration = JsonDeserializationCluster.ServerStoreRestoreGoogleCloudBackupConfiguration(restoreConfiguration);
                    restoreBackupTask = new ServerStoreRestoreFromGoogleCloud(googlCloudConfiguration);
                    break;
                default:
                    throw new InvalidOperationException($"No matching backup type was found for {restoreType}");
            }

            AsyncHelpers.RunSync(()=>restoreBackupTask.Execute(x =>
            {
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    Console.WriteLine(context.ReadObject(x.ToJson(), "ServerStore import").ToString());
               
                }
            }));

            return "ServerStore restore: Completed Successfully";
        }

        public static string Trust(string key, string tag)
        {
            return "Trust is not implemented";
        }

        private static string RecoverServerStoreKey(string srcDir)
        {
            var keyPath = Path.Combine(srcDir, SecretKeyEncrypted);
            if (File.Exists(keyPath) == false)
                throw new IOException("The key file " + keyPath + " doesn't exist. Either the Server Store is not encrypted or you provided a wrong path to the System folder");

            var buffer = File.ReadAllBytes(keyPath);

            var key = new SecretProtection(new SecurityConfiguration()).Unprotect(buffer);
            return Convert.ToBase64String(key);
        }


    }
}
