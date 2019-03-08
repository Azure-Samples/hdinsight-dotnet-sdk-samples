using System;

namespace Microsoft.Azure.Management.HDInsight.Samples
{
    class Program
    {
        enum SampleType
        {
            CreateSparkCluster,
            CreateKafkaCluster,
            CreateHadoopClusterWithAdlsGen2Sample,
            CreateEspCluster
        }

        static void Main(string[] args)
        {
            // Please rename `Configuration.cs.template` to `Configuration.cs` and fill it with correct information.
            // Change the following line to run other samples.
            var sampleType = SampleType.CreateEspCluster;
            switch (sampleType)
            {
                case SampleType.CreateSparkCluster:
                    CreateSparkClusterSample.ClassMain(args);
                    break;

                case SampleType.CreateKafkaCluster:
                    CreateKafkaClusterSample.ClassMain(args);
                    break;

                case SampleType.CreateHadoopClusterWithAdlsGen2Sample:
                    CreateHadoopClusterWithAdlsGen2Sample.ClassMain(args);
                    break;

                case SampleType.CreateEspCluster:
                    CreateEspClusterSample.ClassMain(args);
                    break;

                default: break;
            }

            Console.WriteLine("\nPress ENTER to exit.");
            Console.ReadLine();
        }
    }
}
