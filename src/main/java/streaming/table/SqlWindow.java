package streaming.table;

import java.sql.Timestamp;
import java.util.Properties;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

/**
 * 
 * @author user
 *
 */
public class SqlWindow {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");

		FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>("ui2", new SimpleStringSchema(),
				properties);
//		myConsumer.setStartFromEarliest();

		DataStream<String> ds = env.addSource(myConsumer);
		DataStream<Tuple4<String, String, Long, Integer>> data = ds
				.flatMap(new FlatMapFunction<String, Tuple4<String, String, Long, Integer>>() {

					@Override
					public void flatMap(String value, Collector<Tuple4<String, String, Long, Integer>> out)
							throws Exception {
						if (null != value) {
							String ss[] = value.split("\t");
							if (ss.length == 3) {
								String pro = ss[0];
								String id = ss[1];
								Long time = Long.parseLong(ss[2]);
								out.collect(new Tuple4<String, String, Long, Integer>(pro, id, time, 1));
							}
						}

					}
				}).assignTimestampsAndWatermarks(
						new AssignerWithPunctuatedWatermarks<Tuple4<String, String, Long, Integer>>() {

							@Override
							public long extractTimestamp(Tuple4<String, String, Long, Integer> element,
									long previousElementTimestamp) {
								return element.f2;
							}

							@Override
							public Watermark checkAndGetNextWatermark(Tuple4<String, String, Long, Integer> lastElement,
									long extractedTimestamp) {
								return new Watermark(lastElement.f2);
							}
						});

		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		Table table = tableEnv.fromDataStream(data, "province,id,time,c, proctime.proctime, rowtime.rowtime");
		Table result = tableEnv.sqlQuery(
				"select province,TUMBLE_START(rowtime, INTERVAL '10' SECOND) as wStart, count(distinct id) as uv,count(id) as pv from "
						+ table + "  group by TUMBLE(rowtime, INTERVAL '10' SECOND) , province");

		tableEnv.toRetractStream(result, Result.class).print();

		env.execute();

	}

	public static class Result {
		private Timestamp wStart;
		private String province;
		private Long pv;
		private Long uv;

		public Timestamp getwStart() {
			return wStart;
		}

		public void setwStart(Timestamp wStart) {
			this.wStart = wStart;
		}

		public String getProvince() {
			return province;
		}

		public void setProvince(String province) {
			this.province = province;
		}

		public Long getPv() {
			return pv;
		}

		public void setPv(Long pv) {
			this.pv = pv;
		}

		public Long getUv() {
			return uv;
		}

		public void setUv(Long uv) {
			this.uv = uv;
		}

		@Override
		public String toString() {
			return "Result [wStart=" + wStart + ", province=" + province + ", pv=" + pv + ", uv=" + uv + "]";
		}

	}
}
