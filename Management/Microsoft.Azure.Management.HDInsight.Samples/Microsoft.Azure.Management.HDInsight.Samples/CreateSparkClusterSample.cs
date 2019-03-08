using Microsoft.Azure.Management.HDInsight.Models;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using System;
using System.Collections.Generic;

namespace Microsoft.Azure.Management.HDInsight.Samples
{
    class CreateSparkClusterSample
    {
        internal static void ClassMain(string[] args)
        {
            // Authentication
            var credentials = SdkContext.AzureCredentialsFactory
                .FromServicePrincipal(
                Configurations.ClientId,
                Configurations.ClientSecret,
                Configurations.TenantId,
                AzureEnvironment.AzureGlobalCloud);

            var client = new HDInsightManagementClient(credentials)
            {
                SubscriptionId = Configurations.SubscriptionId
            };

            string clusterName = Configurations.ClusterName;

            // Prepare cluster create parameters
            var createParams = new ClusterCreateParametersExtended
            {
                Location = Configurations.Location,
                Properties = new ClusterCreateProperties
                {
                    ClusterVersion = "3.6",
                    OsType = OSType.Linux,
                    Tier = Tier.Standard,
                    ClusterDefinition = new ClusterDefinition
                    {
                        Kind = "Spark",
                        Configurations = new Dictionary<string, Dictionary<string, string>>()
                        {
                            { "gateway", new Dictionary<string, string>
                                {
                                    { "restAuthCredential.isEnabled", "true" },
                                    { "restAuthCredential.username", Configurations.ClusterLoginUserName },
                                    { "restAuthCredential.password", Configurations.Password }
                                }
                            }
                        }
                    },
                    ComputeProfile = new ComputeProfile
                    {
                        Roles = new List<Role>
                        {
                            new Role
                            {
                                Name = "headnode",
                                TargetInstanceCount = 2,
                                HardwareProfile = new HardwareProfile
                                {
                                    VmSize = "Large"
                                },
                                OsProfile = new OsProfile
                                {
                                    LinuxOperatingSystemProfile = new LinuxOperatingSystemProfile
                                    {
                                        Username = Configurations.SshUserName,
                                        Password = Configurations.Password
                                    }
                                }
                            },
                            new Role
                            {
                                Name = "workernode",
                                TargetInstanceCount = 3,
                                HardwareProfile = new HardwareProfile
                                {
                                    VmSize = "Large"
                                },
                                OsProfile = new OsProfile
                                {
                                    LinuxOperatingSystemProfile = new LinuxOperatingSystemProfile
                                    {
                                        Username = Configurations.SshUserName,
                                        Password = Configurations.Password
                                    }
                                }
                            }
                        }
                    },
                    StorageProfile = new StorageProfile
                    {
                        Storageaccounts = new List<StorageAccount>
                        {
                            new StorageAccount
                            {
                                Name = Configurations.StorageAccountName + Configurations.BlobEndpointSuffix,
                                Key = Configurations.StorageAccountKey,
                                Container = Configurations.ContainerName.ToLowerInvariant(),
                                IsDefault =  true
                            }
                        }
                    }
                }
            };

            Console.WriteLine($"Start to create HDInsight Spark cluster {clusterName}");
            var cluster = client.Clusters.Create(Configurations.ResourceGroupName, clusterName, createParams);
            Console.WriteLine($"Finish creating HDInsight Spark cluster {clusterName}");
        }
    }
}
