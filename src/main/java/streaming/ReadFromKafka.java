package streaming;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

public class ReadFromKafka {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");

		FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>("ui", new SimpleStringSchema(),
				properties);
		myConsumer.setStartFromEarliest();

		DataStream<String> ds = env.addSource(myConsumer);
		ds.print();
		env.execute();

	}

}
