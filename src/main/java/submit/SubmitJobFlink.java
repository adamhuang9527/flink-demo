package submit;

import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public class SubmitJobFlink {
    public static void main(String[] args) throws Exception {


//        String configurationDirectory = "/Users/user/work/flink/conf";
//        Configuration configuration = GlobalConfiguration.loadConfiguration(configurationDirectory);

        Configuration configuration = new Configuration();
        configuration.setString("jobmanager.rpc.address", "tv6-dmomachines-dmotest-01");
        configuration.setString("jobmanager.rpc.port", "39130");
        configuration.setString("rest.port", "45037");


        JobGraph jobGraph = getJobGraph();

        jobGraph.setAllowQueuedScheduling(true);
        CompletableFuture<JobSubmissionResult> result = new RestClusterClient(configuration, "4caec0d").submitJob(jobGraph);

        System.out.println(result.get().getJobID());
    }


    public static JobGraph getJobGraph() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
//        ds.writeAsText("/Users/user/work/flink_data/aaa");

        return env.getStreamGraph().getJobGraph();
    }

}
