package streaming.dashboard;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

/**
 * side output 、table、sql、多sink的使用
 * <p>
 * 功能 1。source从流接收数据，从主流数据中引出side outout进行计算，消费同一个流，避免多次接入，消耗网略 2. 通过table和sql计算pv,平均响应时间,错误率（status不等于200的占比）
 * 3。原始数据输出到hdfs。 4，side output的结果数据发送到csv sink
 */
public class Dashboard1 {

	public static void main(String[] args) throws Exception {

		// traceid,userid,timestamp,status,response time
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Row> dataStream = env.addSource(new GeneData()).map(new MapFunction<String, Row>() {
			@Override
			public Row map(String value) throws Exception {
				Row row = new Row(1);
				row.setField(0, "1");
				return row;
			}
		}).returns(new RowTypeInfo(Types.STRING));

		StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
		tenv.registerDataStream("log", dataStream, "a");

		//write to csv sink

		TableSink csvSink = new CsvTableSink("/Users/user/work/flink_data/log", "|");
		String[] fieldNames = {"a", "b", "c", "d"};
		TypeInformation[] fieldTypes = {Types.LONG, Types.INT, Types.DOUBLE, Types.SQL_TIMESTAMP};

		tenv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, csvSink);

		String sql =
				"select * from log";

		Table table = tenv.sqlQuery(sql);

//        result1.insertInto("CsvSinkTable");

		//write to hdfs sink
//        BucketingSink<Tuple5<String, Integer, Long, Integer, Integer>> sink = new BucketingSink<>("hdfs://localhost/logs/");
//        sink.setUseTruncate(false);
////        sink.setBucketer(new Bucketer<UI>() {
////            @Override
////            public Path getBucketPath(Clock clock, Path basePath, UI element) {
////                String newDateTimeString = dateTimeFormatter.format(Instant.ofEpochMilli(element.getTimestamp()));
////                return new Path(basePath + "/" + newDateTimeString);
////            }
////        });
//        sink.setBucketer(new DateTimeBucketer<Tuple5<String, Integer, Long, Integer, Integer>>("yyyy-MM-dd--HHmm", ZoneId.of("UTC+8")));
//        sink.setWriter(new StringWriter<Tuple5<String, Integer, Long, Integer, Integer>>());
//        sink.setBatchSize(1024 * 1024 * 10); // this is 10 MB,
//        sink.setBatchRolloverInterval(60 * 1000); // this is 1 min
//        ds.addSink(sink);

//        tenv.toAppendStream(result1, Result.class).addSink(sink);

		//输出到控制台
		tenv.toAppendStream(table, Row.class).print();

		env.execute();

	}

	public static class Result {

		private Long pv;
		private Integer avg_res_time;
		private Double errorRate;
		private Timestamp stime;

		public Long getPv() {
			return pv;
		}

		public void setPv(Long pv) {
			this.pv = pv;
		}

		public Integer getAvg_res_time() {
			return avg_res_time;
		}

		public void setAvg_res_time(Integer avg_res_time) {
			this.avg_res_time = avg_res_time;
		}

		public Double getErrorRate() {
			return errorRate;
		}

		public void setErrorRate(Double errorRate) {
			this.errorRate = errorRate;
		}

		public Timestamp getStime() {
			return stime;
		}

		public void setStime(Timestamp stime) {
			this.stime = stime;
		}

		@Override
		public String toString() {
			return "Result{" +
					"pv=" + pv +
					", avg_res_time=" + avg_res_time +
					", errorRate=" + errorRate +
					", stime=" + stime +
					'}';
		}
	}

}
