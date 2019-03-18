package streaming.hdfssink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.hadoop.fs.Path;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * 
 * @author zhangjun
 * @date 2018年12月10日 下午3:29:41
 */
public class Kafka2Hdfs {

	static transient DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
			.withZone(ZoneId.of("UTC+8"));

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.enableCheckpointing(5000);
		env.setStateBackend(new RocksDBStateBackend("hdfs://localhost/checkpoints-data/"));

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");

		FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>("ui3", new SimpleStringSchema(),
				properties);
//		myConsumer.setStartFromEarliest();
//		myConsumer.setStartFromLatest();



		DataStream<UI> ds = env.readTextFile("/Users/user/work/flink_data/ui3.txt").map(new MapFunction<String, UI>() {

			@Override
			public UI map(String value) throws Exception {
				String ss[] = value.split("\t");
				UI ui = new UI();
				ui.setProvince(ss[0]);
				ui.setId(ss[1]);
				ui.setTimestamp(Long.parseLong(ss[2]));
				ui.setDate(ss[3]);
				ui.setCount(Long.parseLong(ss[4]));
				return ui;
			}
		});

		BucketingSink<UI> sink = new BucketingSink<UI>("/Users/user/work/flink_data");

		sink.setUseTruncate(false);

		sink.setBucketer(new Bucketer<UI>() {

			@Override
			public Path getBucketPath(Clock clock, Path basePath, UI element) {
				String newDateTimeString = dateTimeFormatter.format(Instant.ofEpochMilli(element.getTimestamp()));
				return new Path(basePath + "/" + newDateTimeString);
			}
		});
//		sink.setBucketer(new DateTimeBucketer<UI>("yyyy-MM-dd--HHmm", ZoneId.of("UTC+8")));
		sink.setWriter(new StringWriter<UI>());
		sink.setBatchSize(1024 * 1024 * 10); // this is 10 MB,
		sink.setBatchRolloverInterval(60 * 1000); // this is 3 mins

		ds.addSink(sink);
		env.execute();
	}

}
