using Microsoft.Azure.Management.HDInsight.Models;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Microsoft.Azure.Management.HDInsight.Samples
{
    class CreateHadoopClusterWithAdlsGen2Sample
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

            // Parse ADLS Gen2 storage account name from resource id
            string adlsGen2AccountName = Configurations.AdlsGen2ResourceId.Split('/').Last();

            string clusterName = Configurations.ClusterName;

            // Prepare cluster create parameters
            var createParamsForADLSv2 = new ClusterCreateParametersExtended
            {
                Location = Configurations.Location,
                Properties = new ClusterCreateProperties
                {
                    ClusterVersion = "3.6",
                    OsType = OSType.Linux,
                    Tier = Tier.Standard,
                    ClusterDefinition = new ClusterDefinition
                    {
                        Kind = "Hadoop",
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
                                Name = adlsGen2AccountName + Configurations.DfsEndpointSuffix,
                                IsDefault = true,
                                FileSystem = Configurations.AdlsGen2FileSystemName.ToLowerInvariant(),
                                ResourceId = Configurations.AdlsGen2ResourceId,
                                MsiResourceId = Configurations.ManagedIdentityResourceId
                            }
                        }
                    }
                },
                Identity = new ClusterIdentity
                {
                    Type = ResourceIdentityType.UserAssigned,
                    UserAssignedIdentities = new Dictionary<string, ClusterIdentityUserAssignedIdentitiesValue>
                    {
                        { Configurations.ManagedIdentityResourceId, new ClusterIdentityUserAssignedIdentitiesValue() }
                    }
                }
            };

            Console.WriteLine($"Start to create HDInsight Hadoop cluster {clusterName} with Azure Data Lake Storage Gen2");
            client.Clusters.Create(Configurations.ResourceGroupName, clusterName, createParamsForADLSv2);
            Console.WriteLine($"Finish creating HDInsight Hadoop cluster {clusterName} with Azure Data Lake Storage Gen2");
        }
    }
}
