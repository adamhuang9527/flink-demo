package streaming.split;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import streaming.mysqlsink.MysqlSink;

public class SplitDemo {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test1");

		FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>("ui1", new SimpleStringSchema(),
				properties);
//		myConsumer.setStartFromEarliest();

		DataStream<String> ds = env.addSource(myConsumer);


		
		DataStream<Tuple2<String, Long>> data = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {

			@Override
			public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
				if (null != value) {
					String ss[] = value.split("\t");
					if (ss.length == 2) {
						String id = ss[0];
						Long time = Long.parseLong(ss[1]);
						out.collect(new Tuple2<String, Long>(id, time));
					}
				}

			}
		});
		

		SplitStream<Tuple2<String, Long>> split = data.split(new OutputSelector<Tuple2<String, Long>>() {

			@Override
			public Iterable<String> select(Tuple2<String, Long> value) {
				int v = Integer.parseInt(value.f0.split("-")[1]);
				List<String> output = new ArrayList<String>();
				if (v % 2 == 0) {
					output.add("even");
				} else {
					output.add("odd");
				}
				return output;
			}
		});

		DataStream<Tuple2<String, Long>> even = split.select("even");
		DataStream<Tuple2<String, Long>> odd = split.select("odd");
		DataStream<Tuple2<String, Long>> all = split.select("even", "odd");

		odd.print();
		
//		even.addSink(new MysqlSink());

		env.execute("kafka2mysql");

	}

}
