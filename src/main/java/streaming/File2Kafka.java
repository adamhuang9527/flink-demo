package streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

/**
 * write file data to kafka
 * 
 * @author user
 *
 */
public class File2Kafka {
	public static void main(String[] args) throws Exception {
		
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream ds = env.readTextFile("/Users/user/work/flink_data/ui.csv");

		FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>("localhost:9092", // broker list
				"ui5", // target topic
				new SimpleStringSchema()); // serialization schema

		ds.map(new MapFunction<String, String>() {

			@Override
			public String map(String value) throws Exception {
				String ss[] = value.split("\t");
				return System.currentTimeMillis() + "\t" + ss[1];
			}
		}).addSink(myProducer);
		env.execute();
	}
}
