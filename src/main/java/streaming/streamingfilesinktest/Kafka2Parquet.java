package streaming.streamingfilesinktest;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

public class Kafka2Parquet {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");


        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010("kafkaSourceJson", new SimpleStringSchema(),
                properties);
        myConsumer.setStartFromLatest();

        StreamingFileSink<Address> sink = StreamingFileSink.forBulkFormat(
                new Path("file:///tmp/flink-data/a"),
                ParquetAvroWriters.forSpecificRecord(Address.class))
                .build();

        DataStream ds = env.addSource(myConsumer).map(new MapFunction<String, Address>() {
            @Override
            public Address map(String value) throws Exception {
                JSONObject json = JSONObject.parseObject(value);
                Address address  = new Address();
                address.setNum(json.getIntValue("num"));
                address.setStreet(json.getString("street"));
                address.setCity(json.getString("city"));
                address.setState(json.getString("state"));
                address.setZip(json.getString("zip"));

                return address;
            }
        });
        ds.addSink(sink);
        ds.print();

        env.execute();

    }
}
