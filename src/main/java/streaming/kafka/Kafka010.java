package streaming.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

public class Kafka010 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000);
        env.setStateBackend(new RocksDBStateBackend("file:///tmp/checkpoints-data/"));

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "kafkaSourceJson");

        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>("test3", new SimpleStringSchema(),
                properties);
        myConsumer.setStartFromTimestamp(1562574960000L);
        env.addSource(myConsumer).print();
        env.execute();
    }
}

