package submit;

import org.apache.commons.cli.CommandLine;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class SubmitJobPerJobYarnFromJar {


    public static void main(String[] args) throws Exception {


        //设置参数
        InputParams options = new InputParams();
        options.setJarFilePath("/Users/user/work/flink/examples/streaming/WordCount.jar");
//        String programArgs[] = new String[]{"--port", "9999"};
//        options.setProgramArgs(programArgs);
        options.setParallelism(2);

        //start
        SubmitJobPerJobYarnFromJar submit = new SubmitJobPerJobYarnFromJar();
        Result result = submit.submitJob(options);

        //response

        ClusterClient clusterClient = result.getClusterClient();
        JobID jobId = result.getJobId();


        ApplicationId applicationId = clusterClient.getClusterId();
        final RestClusterClient<ApplicationId> restClusterClient = (RestClusterClient<ApplicationId>) clusterClient;
        JobDetailsInfo jobDetailsInfo = restClusterClient.getJobDetails(jobId).get();
        final CompletableFuture<JobResult> jobResultCompletableFuture = restClusterClient.requestJobResult(jobId);
        final JobResult jobResult = jobResultCompletableFuture.get();
        System.out.println(jobResult.getApplicationStatus());
//        restClusterClient.cancel(jobId);
        System.out.println("shutdown ");

    }


    Result submitJob(InputParams options) throws FileNotFoundException, ProgramInvocationException {
        PackagedProgram program = this.buildProgram(options);
        String configurationDirectory = "/Users/user/work/flink/conf";
        Configuration configuration = GlobalConfiguration.loadConfiguration(configurationDirectory);

        int parallelism = options.getParallelism() == null ? 1 : options.getParallelism();

        JobGraph jobGraph = PackagedProgramUtils.createJobGraph(program, configuration, parallelism);


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
        yarnClusterDescriptor.setLocalJarPath(new Path("/Users/user/work/flink/lib/flink-dist_2.12-1.8.0.jar"));
        File flinkLibFolder = new File("/Users/user/work/flink/lib");
        yarnClusterDescriptor.addShipFiles(Arrays.asList(flinkLibFolder.listFiles()));


        File testingJar = new File(options.getJarFilePath());
        jobGraph.addJar(new org.apache.flink.core.fs.Path(testingJar.toURI()));

        ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
                .setMasterMemoryMB(1024)
                .setTaskManagerMemoryMB(
                        1024)
                .setNumberTaskManagers(1)
                .setSlotsPerTaskManager(1)
                .createClusterSpecification();

        yarnClusterDescriptor.setName(options.getJobName());


        ClusterClient<ApplicationId> clusterClient = yarnClusterDescriptor.deployJobCluster(clusterSpecification,
                jobGraph,
                false);


        return new Result(jobGraph.getJobID(), clusterClient);

    }

    public static class Result {
        private JobID jobId;
        private ClusterClient clusterClient;

        public Result(JobID jobId, ClusterClient clusterClient) {
            this.jobId = jobId;
            this.clusterClient = clusterClient;
        }

        public JobID getJobId() {
            return jobId;
        }

        public void setJobId(JobID jobId) {
            this.jobId = jobId;
        }

        public ClusterClient getClusterClient() {
            return clusterClient;
        }

        public void setClusterClient(ClusterClient clusterClient) {
            this.clusterClient = clusterClient;
        }
    }

    private PackagedProgram buildProgram(InputParams options) throws FileNotFoundException, ProgramInvocationException {
        String[] programArgs = options.getProgramArgs();
        String jarFilePath = options.getJarFilePath();
        List<URL> classpaths = options.getClasspaths();

        if (jarFilePath == null) {
            throw new IllegalArgumentException("The program JAR file was not specified.");
        }

        File jarFile = new File(jarFilePath);

        // Check if JAR file exists
        if (!jarFile.exists()) {
            throw new FileNotFoundException("JAR file does not exist: " + jarFile);
        } else if (!jarFile.isFile()) {
            throw new FileNotFoundException("JAR file is not a file: " + jarFile);
        }

        // Get assembler class
        String entryPointClass = options.getEntryPointClass();

        PackagedProgram program = entryPointClass == null ?
                new PackagedProgram(jarFile, classpaths, programArgs) :
                new PackagedProgram(jarFile, classpaths, entryPointClass, programArgs);

        program.setSavepointRestoreSettings(this.createSavepointRestoreSettings(options));

        return program;
    }


    private SavepointRestoreSettings createSavepointRestoreSettings(InputParams options) {
        if (options.getFromSavepoint() != null) {
            String savepointPath = options.getFromSavepoint();
            boolean allowNonRestoredState = options.isAllowNonRestoredState();
            return SavepointRestoreSettings.forPath(savepointPath, allowNonRestoredState);
        } else {
            return SavepointRestoreSettings.none();
        }
    }

}
