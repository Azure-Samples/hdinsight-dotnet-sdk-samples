using Microsoft.Azure.Management.HDInsight.Models;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Microsoft.Azure.Management.HDInsight.Samples
{
    class CreateEspClusterSample
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

            // Parse AAD-DS DNS Domain name from resource id
            string aaddsDnsDomainName = Configurations.AaddsResourceId.Split('/').Last();

            // Prepare cluster create parameters
            var createParams = new ClusterCreateParametersExtended
            {
                Location = Configurations.Location,
                Properties = new ClusterCreateProperties
                {
                    ClusterVersion = "3.6",
                    OsType = OSType.Linux,
                    Tier = Tier.Premium,
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
                                },
                                VirtualNetworkProfile = new VirtualNetworkProfile
                                {
                                    Id = Configurations.VirtualNetworkResourceId,
                                    Subnet = string.Format("{0}/subnets/{1}", Configurations.VirtualNetworkResourceId, Configurations.SubnetName)
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
                                },
                                VirtualNetworkProfile = new VirtualNetworkProfile
                                {
                                    Id = Configurations.VirtualNetworkResourceId,
                                    Subnet = string.Format("{0}/subnets/{1}", Configurations.VirtualNetworkResourceId, Configurations.SubnetName)
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
                    },
                    SecurityProfile = new SecurityProfile
                    {
                        DirectoryType = DirectoryType.ActiveDirectory,
                        LdapsUrls = new[] { Configurations.LdapsUrl },
                        DomainUsername = Configurations.DomainUserName,
                        Domain = aaddsDnsDomainName,
                        ClusterUsersGroupDNs = new[] { Configurations.ClusterAccessGroup },
                        AaddsResourceId = Configurations.AaddsResourceId,
                        MsiResourceId = Configurations.ManagedIdentityResourceId
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

            Console.WriteLine($"Starting to create HDInsight Spark cluster {clusterName} with Enterprise Security Package");
            client.Clusters.Create(Configurations.ResourceGroupName, clusterName, createParams);
            Console.WriteLine($"Finished creating HDInsight Spark cluster {clusterName} with Enterprise Security Package");
        }
    }
}
