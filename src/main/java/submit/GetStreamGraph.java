package submit;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
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
import org.apache.flink.streaming.connectors.kafka.*;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.*;

/**
 * 通过jar 包提取出source和sink信息
 */
public class GetStreamGraph {

//    private Map<Integer, List<Integer>> sinkSources = new HashMap<>();
//    private Map<Integer, Map<String, Object>> sourceSinkInfoMap = new HashMap<>();

    public static void main(String[] args)  {


        InputParams options = new InputParams();
        options.setJarFilePath("/Users/user/git/flink-platform/target/flink-platform-1.0.jar");
//        options.setEntryPointClass("platform.KafkaTest");
        options.setParallelism(1);
        options.setJobName("myflinkjob-udf");
        options.setProgramArgs(new String[]{"select * from OrderB"});



        GetStreamGraph gsg = new GetStreamGraph();


        ResultMsg resultMsg = null;
        try {
            resultMsg = gsg.getGraph(options);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println(resultMsg.getSinkSources());
        System.out.println(resultMsg.getSourceSinkInfoMap());


    }


    public ResultMsg getGraph(InputParams options) throws Exception {

        Map<Integer, List<Integer>> sinkSources = new HashMap<>();
        Map<Integer, Map<String, Object>> sourceSinkInfoMap = new HashMap<>();


        String configurationDirectory = "/Users/user/work/flink/conf";
        Configuration configuration = GlobalConfiguration.loadConfiguration(configurationDirectory);
        Optimizer optimizer = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), configuration);
        OptimizerPlanEnvironment optimizerPlanEnvironment = new OptimizerPlanEnvironment(optimizer);
        optimizerPlanEnvironment.setParallelism(1);
        PackagedProgram program = this.buildProgram(options);
        FlinkPlan flinkPlan = optimizerPlanEnvironment.getOptimizedPlan(program);
        if (flinkPlan instanceof StreamGraph) {
            StreamGraph streamGraph = (StreamGraph) flinkPlan;


            String json = streamGraph.getStreamingPlanAsJSON();
            getRelationship(sinkSources,json);


            Collection<Integer> sourceIds = streamGraph.getSourceIDs();

            for (Integer sourceid : sourceIds) {
                sourceSinkInfoMap.put(sourceid, new HashMap<>());


                StreamNode streamNode = streamGraph.getStreamNode(sourceid);
                StreamOperator streamOperator = streamNode.getOperator();
                if (streamOperator instanceof StreamSource) {
                    StreamSource streamSource = (StreamSource) streamOperator;
                    Function function = streamSource.getUserFunction();
                    if (function instanceof FlinkKafkaConsumerBase) {
                        Class class09 = null;

                        if (function instanceof FlinkKafkaConsumer010) {
                            class09 = function.getClass().getSuperclass();
                        } else if (function instanceof FlinkKafkaConsumer09) {
                            class09 = function.getClass();
                        } else if (function instanceof FlinkKafkaConsumer011) {
                            class09 = function.getClass().getSuperclass().getSuperclass();
                        }


                        Field propertiesField = class09.getDeclaredField("properties");
                        propertiesField.setAccessible(true);
                        Properties properties = (Properties) propertiesField.get(function);
                        System.out.println(properties);

                        Class class10 = class09.getSuperclass();
                        Field fieldTopics = class10.getDeclaredField("topicsDescriptor");
                        fieldTopics.setAccessible(true);
                        KafkaTopicsDescriptor kafkaTopicsDescriptor = (KafkaTopicsDescriptor) fieldTopics.get(function);
                        System.out.println(" topics " + kafkaTopicsDescriptor.getFixedTopics());

                        sourceSinkInfoMap.get(sourceid).put("properties", properties);
                        sourceSinkInfoMap.get(sourceid).put("topics", kafkaTopicsDescriptor.getFixedTopics());

                    }


                }
            }


            Collection<Integer> sinkIDs = streamGraph.getSinkIDs();
            for (Integer sinkId : sinkIDs) {

                StreamNode streamNode = streamGraph.getStreamNode(sinkId);
                StreamOperator streamOperator = streamNode.getOperator();
                if (streamOperator instanceof StreamSink) {
                    StreamSink streamSink = (StreamSink) streamOperator;
                    Function function = streamSink.getUserFunction();
                    if (function instanceof BucketingSink) {
                        sourceSinkInfoMap.put(sinkId, new HashMap<>());
                        Field basePathField = function.getClass().getDeclaredField("basePath");
                        basePathField.setAccessible(true);
                        String basePath = (String) basePathField.get(function);
                        System.out.println(basePath);

                        sourceSinkInfoMap.get(sinkId).put("basePath", basePath);

                    } else if (function instanceof FlinkKafkaProducerBase || function instanceof FlinkKafkaProducer011) {
                        sourceSinkInfoMap.put(sinkId, new HashMap<>());
                        Class baseClass = null;

                        if (function instanceof FlinkKafkaProducer010) {
                            baseClass = function.getClass().getSuperclass().getSuperclass();
                        } else if (function instanceof FlinkKafkaProducer09) {
                            baseClass = function.getClass().getSuperclass();
                        } else if (function instanceof FlinkKafkaProducer011) {
                            baseClass = function.getClass();

                        }

                        Field propertiesField = baseClass.getDeclaredField("producerConfig");
                        propertiesField.setAccessible(true);
                        Properties properties = (Properties) propertiesField.get(function);
                        System.out.println(" sink properties:  " + properties);

                        Field topicField = baseClass.getDeclaredField("defaultTopicId");
                        topicField.setAccessible(true);
                        String topic = (String) topicField.get(function);
                        System.out.println("sink topic :" + topic);

                        sourceSinkInfoMap.get(sinkId).put("properties", properties);
                        sourceSinkInfoMap.get(sinkId).put("topic", topic);
                    }
                }

            }


        }


        ResultMsg result = new ResultMsg();
        result.setSinkSources(sinkSources);
        result.setSourceSinkInfoMap(sourceSinkInfoMap);

        return result;
    }


    private void getParents(Map<Integer, List<Integer>> sinkSources,int sinkId, int currentId, Map<Integer, JSONObject> nodes) {
        //递归结束
        JSONObject node = nodes.get(currentId);
        if (!node.containsKey("predecessors") && node.getString("pact").equals("Data Source")) {
            sinkSources.get(sinkId).add(node.getInteger("id"));
            return;
        } else {
            JSONArray predecessors = node.getJSONArray("predecessors");
            for (int i = 0; i < predecessors.size(); i++) {
                JSONObject predecessor = predecessors.getJSONObject(i);
                int nid = predecessor.getInteger("id");
                getParents(sinkSources,sinkId, nid, nodes);
            }
        }

    }

    private void getRelationship(Map<Integer, List<Integer>> sinkSources,String json) {
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONArray jsonArray = jsonObject.getJSONArray("nodes");

        Map<Integer, JSONObject> nodes = new HashMap<>();
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject node = jsonArray.getJSONObject(i);
            nodes.put(node.getInteger("id"), node);


            if ("Data Sink".equals(node.getString("pact"))) {
                int sinkid = node.getInteger("id");
                sinkSources.put(sinkid, new ArrayList<>());
                getParents(sinkSources,sinkid, sinkid, nodes);
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


    public static class ResultMsg {
        private Map<Integer, List<Integer>> sinkSources = new HashMap<>();
        private Map<Integer, Map<String, Object>> sourceSinkInfoMap = new HashMap<>();

        public Map<Integer, List<Integer>> getSinkSources() {
            return sinkSources;
        }

        public void setSinkSources(Map<Integer, List<Integer>> sinkSources) {
            this.sinkSources = sinkSources;
        }

        public Map<Integer, Map<String, Object>> getSourceSinkInfoMap() {
            return sourceSinkInfoMap;
        }

        public void setSourceSinkInfoMap(Map<Integer, Map<String, Object>> sourceSinkInfoMap) {
            this.sourceSinkInfoMap = sourceSinkInfoMap;
        }
    }
}
