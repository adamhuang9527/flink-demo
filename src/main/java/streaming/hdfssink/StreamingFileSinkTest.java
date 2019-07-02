package streaming.hdfssink;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author zhangjun
 * @date 2018年12月10日 下午3:29:41
 */
public class StreamingFileSinkTest {

    static Logger LOG = LoggerFactory.getLogger(StreamingFileSinkTest.class);

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
        properties.setProperty("bootstrap.servers", "localhost:9092,localhost:9093");
        properties.setProperty("group.id", "test3");


        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>("test3", new SimpleStringSchema(),
                properties);


        DataStream<User> ds = env.addSource(myConsumer, "read from kafka")
                .map((MapFunction<String, User>) s -> {
                    User order = JSONObject.parseObject(s, User.class);
                    return order;
                });


//        StreamingFileSink<User> sink = StreamingFileSink.forBulkFormat(
//                new Path("hdfs://localhost/flink-data/"),
//                ParquetAvroWriters.forReflectRecord(User.class))
//                .withBucketCheckInterval(5 * 60 * 1000)
//                .build();


        StreamingFileSink<User> sink = StreamingFileSink
                .forRowFormat(new Path("hdfs://localhost/flink-data/"), new SimpleStringEncoder<User>("UTF-8"))
                .withBucketCheckInterval(2*60*1000)
                .withBucketAssigner(new BucketAssigner<User, String>() {
                    @Override
                    public String getBucketId(User element, Context context) {
                        return null;
                    }

                    @Override
                    public SimpleVersionedSerializer<String> getSerializer() {
                        return null;
                    }
                })
                .build();


        ds.addSink(sink);
        env.execute();
    }

    public static class User implements java.io.Serializable {
        private String appName;

        private String clientIp;

        private long uploadTime;

        public String getAppName() {
            return appName;
        }

        public void setAppName(String appName) {
            this.appName = appName;
        }

        public String getClientIp() {
            return clientIp;
        }

        public void setClientIp(String clientIp) {
            this.clientIp = clientIp;
        }

        public long getUploadTime() {
            return uploadTime;
        }

        public void setUploadTime(long uploadTime) {
            this.uploadTime = uploadTime;
        }
    }

}
