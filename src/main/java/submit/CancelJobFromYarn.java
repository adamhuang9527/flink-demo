package submit;


import org.apache.flink.api.common.JobID;
import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.util.FlinkException;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * bin/flink cancel -m yarn-cluster -yid application_1557026877487_0043  6aee90ca64986c55a3a62ae7d9312ffe
 */
public class CancelJobFromYarn {
    private static final Logger LOG = LoggerFactory.getLogger(CancelJobFromYarn.class);

    public static void main(String[] args) throws FlinkException, CliArgsException {
        String applicationId = "application_1557026877487_0044";
        String jobId = "fcfba45612b4bc05754f6bb2815b1e8c";

        String configurationDirectory = "/Users/user/work/flink/conf";

        Configuration configuration = GlobalConfiguration.loadConfiguration(configurationDirectory);
        YarnClient yarnClient = YarnClient.createYarnClient();
        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();


        YarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor(
                configuration,
                yarnConfiguration,
                configurationDirectory,
                yarnClient,
                true);

        ApplicationId appId = ConverterUtils.toApplicationId(applicationId);

        try {
            ClusterClient clusterClient = yarnClusterDescriptor.retrieve(appId);

            JobID jobID = parseJobId(jobId);

            try {
                clusterClient.cancel(jobID);
            } catch (Exception e) {
                throw new FlinkException("Could not cancel job " + jobID + '.', e);
            } finally {
                try {
                    clusterClient.shutdown();
                } catch (Exception e) {
                    LOG.info("Could not properly shut down the cluster client.", e);
                }
            }
        } finally {
            try {
                yarnClusterDescriptor.close();
            } catch (Exception e) {
                LOG.info("Could not properly close the cluster descriptor.", e);
            }
        }


    }

    private static JobID parseJobId(String jobIdString) throws CliArgsException {
        JobID jobId;
        try {
            jobId = JobID.fromHexString(jobIdString);
        } catch (IllegalArgumentException e) {
            throw new CliArgsException(e.getMessage());
        }
        return jobId;
    }
}
