package streaming.hdfssink;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;



public class Kafka2Hdfs {
	public static void main(String[] args) {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");

		FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>("ui2", new SimpleStringSchema(),
				properties);

		DataStream<String> ds = env.addSource(myConsumer);

		
		
		BucketingSink<String> sink = new BucketingSink<String>("/base/path");
		
//		BucketingSink<String> sink = new BucketingSink<String>("/base/path");
//		sink.setBucketer(new DateTimeBucketer<String>("yyyy-MM-dd--HHmm", ZoneId.of("America/Los_Angeles")));
//		sink.setWriter(new SequenceFileWriter<IntWritable, Text>());
//		sink.setBatchSize(1024 * 1024 * 400); // this is 400 MB,
//		sink.setBatchRolloverInterval(20 * 60 * 1000); // this is 20 mins

//		ds.addSink(sink);
	}
}
