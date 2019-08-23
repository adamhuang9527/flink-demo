package streaming.streamingfilesinktest;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.apache.commons.lang3.StringUtils;
import streaming.GeneUISource;

import java.util.List;

public class WithSplit {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<List> ds = env.addSource(new GeneUISource());
//		ds.print();
		DataStream<Tuple5<String, String, Long, String, String>> data = ds
				.map(new MapFunction<List, Tuple5<String, String, Long, String, String>>() {
					@Override
					public Tuple5<String, String, Long, String, String> map(List value) throws Exception {
						return new Tuple5<String, String, Long, String, String>(value.get(0).toString(),
								value.get(1).toString(), Long.parseLong(value.get(2).toString()),
								value.get(3).toString(), value.get(4).toString());
					}
				});
//
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		Table table = tableEnv.fromDataStream(data, "province,id,datestamp,date,c");
		Table result = tableEnv.sqlQuery("select * from " + table);
//

		StreamingFileSink<String> sink = StreamingFileSink
				.forRowFormat(new Path("hdfs:///tmp/flimk-data"),
						new SimpleStringEncoder<String>("UTF-8"))
				.withBucketAssigner(new DateTimeBucketAssigner("yyyy-MM-dd"))
				.build();

		byte b1[] = {0x01};
		final String str1 = new String(b1);

		tableEnv.toAppendStream(result, Row.class).map(new MapFunction<Row, String>() {

			@Override
			public String map(Row row) throws Exception {
				int size = row.getArity();
				String s[] = new String[size];
				for (int i = 0; i < size; i++) {
					s[i] = row.getField(i) == null ? null : row.getField(i).toString();
				}

				return StringUtils.join(s, str1);
			}
		}).print();

		env.execute("StreamSql");
	}
}
