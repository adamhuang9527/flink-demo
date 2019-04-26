package submit;

import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
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
import java.util.concurrent.ExecutionException;

public class SubmitJobPerJobYarnFromJar {


    public static void main(String[] args) throws FileNotFoundException, ProgramInvocationException, ClusterDeploymentException, ExecutionException, InterruptedException {

        InputParams options = new InputParams();
        options.setJarFilePath("/Users/user/work/flink/examples/streaming/SocketWindowWordCount.jar");
        String programArgs[] = new String[]{"--port", "9999"};

        options.setProgramArgs(programArgs);

        SubmitJobPerJobYarnFromJar submit = new SubmitJobPerJobYarnFromJar();
        PackagedProgram program = submit.buildProgram(options);

        String configurationDirectory = "/Users/user/work/flink/conf";
        Configuration configuration = GlobalConfiguration.loadConfiguration(configurationDirectory);

        JobGraph jobGraph = PackagedProgramUtils.createJobGraph(program, configuration, 1);

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

        yarnClusterDescriptor.setName("myjob");



        ClusterClient<ApplicationId> clusterClient = yarnClusterDescriptor.deployJobCluster(clusterSpecification,
                jobGraph,
                false);


        ApplicationId applicationId = clusterClient.getClusterId();


        System.out.println(applicationId);
        final RestClusterClient<ApplicationId> restClusterClient = (RestClusterClient<ApplicationId>) clusterClient;

        final CompletableFuture<JobResult> jobResultCompletableFuture = restClusterClient.requestJobResult(jobGraph.getJobID());


        final JobResult jobResult = jobResultCompletableFuture.get();


        System.out.println(jobResult.getApplicationStatus());
    }


    PackagedProgram buildProgram(InputParams options) throws FileNotFoundException, ProgramInvocationException {
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

//        program.setSavepointRestoreSettings(options.getSavepointRestoreSettings());

        return program;
    }


}
