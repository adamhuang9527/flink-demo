package submit;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.FlinkException;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

public class SubmitJobPerJobYarn {
    public static void main(String[] args) throws FlinkException, ExecutionException, InterruptedException {
        YarnClient yarnClient = YarnClient.createYarnClient();
        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();




        String configurationDirectory = "/Users/user/work/flink/conf";
        Configuration configuration = GlobalConfiguration.loadConfiguration(configurationDirectory);

//        FlinkYarnSessionCli cli = new FlinkYarnSessionCli(configuration, configurationDirectory, "y", "yarn");


        YarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor(
                configuration,
                yarnConfiguration,
                configurationDirectory,
                yarnClient,
                false);
//        yarnClusterDescriptor.setLocalJarPath(new Path(""));
        yarnClusterDescriptor.setLocalJarPath(new Path("/Users/user/work/flink/lib/flink-dist_2.12-1.8.0.jar"));
        File flinkLibFolder = new File("/Users/user/work/flink/lib");
        yarnClusterDescriptor.addShipFiles(Arrays.asList(flinkLibFolder.listFiles()));


        JobGraph jobGraph = getJobGraph();




        File testingJar = new File("/Users/user/git/flink-demo/target/Flink-demo-1.0-SNAPSHOT.jar");

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
                true);



        ApplicationId applicationId = clusterClient.getClusterId();

        final RestClusterClient<ApplicationId> restClusterClient = (RestClusterClient<ApplicationId>) clusterClient;

        final CompletableFuture<JobResult> jobResultCompletableFuture = restClusterClient.requestJobResult(jobGraph.getJobID());

        final JobResult jobResult = jobResultCompletableFuture.get();


        System.out.println(applicationId);
        System.out.println(jobResult);
    }


    public static JobGraph getJobGraph() {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        DataStream<String> ds = env.addSource(new SourceFunction<String>() {

            private volatile boolean isRunning = true;
            AtomicLong al = new AtomicLong();

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (isRunning) {
                    Thread.sleep(1);
                    ctx.collect("value" + al.incrementAndGet());
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });


        ds.print();

        return env.getStreamGraph().getJobGraph();
    }

}
