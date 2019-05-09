package submit;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.client.program.OptimizerPlanEnvironment;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.plan.FlinkPlan;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;
import java.util.Collection;
import java.util.List;

/**
 * 通过jar 包提取出source和sink信息
 */
public class GetStreamGraph {
    public static void main(String[] args) throws Exception {


        InputParams options = new InputParams();
        options.setJarFilePath("/Users/user/git/flink-platform/target/flink-platform-1.0.jar");
        options.setParallelism(1);
        options.setJobName("myflinkjob-udf");

        GetStreamGraph gsg = new GetStreamGraph();
        gsg.getGraph(options);


    }


    public void getGraph(InputParams options) throws Exception {

        String configurationDirectory = "/Users/user/work/flink/conf";
        Configuration configuration = GlobalConfiguration.loadConfiguration(configurationDirectory);
        Optimizer optimizer = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), configuration);
        OptimizerPlanEnvironment optimizerPlanEnvironment = new OptimizerPlanEnvironment(optimizer);
        optimizerPlanEnvironment.setParallelism(1);
        PackagedProgram program = this.buildProgram(options);
        FlinkPlan flinkPlan = optimizerPlanEnvironment.getOptimizedPlan(program);
        if (flinkPlan instanceof StreamGraph) {
            StreamGraph streamGraph = (StreamGraph) flinkPlan;
            Collection<Integer> sourceIds = streamGraph.getSourceIDs();

            for (Integer sourceid : sourceIds) {
                StreamNode streamNode = streamGraph.getStreamNode(sourceid);
                StreamOperator streamOperator = streamNode.getOperator();
                if (streamOperator instanceof StreamSource) {
                    StreamSource streamSource = (StreamSource) streamOperator;
                    Function function = streamSource.getUserFunction();
                    if (function instanceof FlinkKafkaConsumer09) {
                        FlinkKafkaConsumer09 flinkKafkaConsumer09 = (FlinkKafkaConsumer09) function;
                        System.out.println(flinkKafkaConsumer09);
                    }
                }
            }


            Collection<Integer> sinkIDs = streamGraph.getSinkIDs();
            for (Integer sourceid : sinkIDs) {
                StreamNode streamNode = streamGraph.getStreamNode(sourceid);
                StreamOperator streamOperator = streamNode.getOperator();
                if (streamOperator instanceof StreamSink) {
                    StreamSink streamSink = (StreamSink) streamOperator;
                    Function function = streamSink.getUserFunction();
                    if (function instanceof BucketingSink) {
                        BucketingSink bucketingSink = (BucketingSink) function;
                        System.out.println(bucketingSink);
                    } else if (function instanceof FlinkKafkaProducerBase) {
                        FlinkKafkaProducerBase flinkKafkaProducerBase = (FlinkKafkaProducerBase) function;
                        System.out.println(flinkKafkaProducerBase);
                    }
                    System.out.println(function);
                }

            }


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
