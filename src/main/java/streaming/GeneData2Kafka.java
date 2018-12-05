package streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

/**
 * write file data to kafka
 * 
 * @author user
 *
 */
public class GeneData2Kafka {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> source = env.addSource(new SourceFunction<String>() {

			private volatile boolean isRunning = true;
			private static final long serialVersionUID = 1L;

			@Override
			public void run(SourceContext<String> ctx) throws Exception {
				while (isRunning) {
					Thread.sleep(10);
					ctx.collect("id-" + (int) (Math.random() * 1000) + "\t" + System.currentTimeMillis());
				}
			}

			@Override
			public void cancel() {
				isRunning = false;
			}
		});

//		source.print();

		FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>("localhost:9092", // broker list
				args[0], // target topic
				new SimpleStringSchema()); // serialization schema
		source.addSink(myProducer);
		env.execute();
	}
}
