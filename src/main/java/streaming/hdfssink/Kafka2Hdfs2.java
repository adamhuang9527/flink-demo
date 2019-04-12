package streaming.hdfssink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.Properties;

/**
 * @author zhangjun
 * @date 2018年12月10日 下午3:29:41
 */
public class Kafka2Hdfs2 {

    static Logger LOG = LoggerFactory.getLogger(Kafka2Hdfs2.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        env.setStateBackend(new RocksDBStateBackend("hdfs://localhost/checkpoints-data/"));


        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        config.setMinPauseBetweenCheckpoints(500);
        config.setCheckpointTimeout(60000);
        config.setMaxConcurrentCheckpoints(1);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");


        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>("test1", new SimpleStringSchema(),
                properties);
//		myConsumer.setStartFromEarliest();
//		myConsumer.setStartFromLatest();


        DataStream<String> ds = env.addSource(myConsumer,"read from kafka");
//        .map(new MapFunction<String, String>() {
//            @Override
//            public String map(String s) throws Exception {
//                if (s.equals("value_8005001")) {
//                    System.exit(0);

//                }
//
//                return s;
//            }
//        });


        BucketingSink<String> sink = new BucketingSink<String>("hdfs://localhost/flink_data");
        sink.setBucketer(new DateTimeBucketer<String>("yyyy-MM-dd", ZoneId.of("UTC+8")));
        sink.setWriter(new StringWriter<String>());
        sink.setBatchSize(1024 * 1024 * 10); // this is 10 MB,
        sink.setBatchRolloverInterval(10 * 60 * 1000); // this is 3 mins

        ds.addSink(sink);
        ds.print();
        env.execute();
    }

}
