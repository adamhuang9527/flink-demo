package streaming.window;

import java.util.Properties;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import batch.UI;

public class SlidingWindows {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
		

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");

		FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>("ui7", new SimpleStringSchema(),
				properties);
		myConsumer.setStartFromEarliest();

		DataStream<String> ds = env.addSource(myConsumer);
		DataStream<Tuple2<String, Integer>> counts = ds.flatMap(new FlatMapFunction<String, batch.UI>() {

			@Override
			public void flatMap(String value, Collector<UI> out) throws Exception {
				String ss[] = value.split("\t");
				if (ss.length > 1) {
					UI ui = new UI();
					ui.setId(ss[0]);
					ui.setDatestamp(Long.parseLong(ss[1]));
					out.collect(ui);
				}

			}
		}).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<UI>() {

			@Override
			public long extractTimestamp(UI element, long previousElementTimestamp) {
				return element.getDatestamp();
			}

			@Override
			public Watermark checkAndGetNextWatermark(UI lastElement, long extractedTimestamp) {
				return new Watermark(lastElement.getDatestamp());
			}
		}).flatMap(new FlatMapFunction<UI, Tuple2<String, Integer>>() {
			@Override
			public void flatMap(UI value, Collector<Tuple2<String, Integer>> out) throws Exception {
				out.collect(new Tuple2<String, Integer>(value.getId(), 1));
			}
		}).keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(10))).sum(1);

		counts.writeAsText("/Users/user/work/flink_data/ui7_result");
		env.execute();

	}

}
