package streaming.checkpoint;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import streaming.dashboard.GeneData;

public class MyTest2 {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		DataStream<String> dataStream = env.addSource(new GeneData());
		DataStream<Tuple3<String, String, String>> dataStream2 = dataStream.map(
				(MapFunction<String, Tuple3<String, String, String>>) value -> {
					String ss[] = value.split(",");
					return new Tuple3(ss[0], ss[2], "2019-08-08 12:57:28");
				}).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING));

		tableEnv.registerDataStream("ubt", dataStream2, "uuid,intotime,t,proctime.proctime");

		String sql="select SUBSTRING(t,1,10),SUBSTRING(t,12,5) from ubt";
		Table table = tableEnv.sqlQuery(sql);
//		Table table = tableEnv.sqlQuery(
//				"select SUBSTRING(data,3,5) as uuid,TUMBLE_START(proctime, INTERVAL '1' SECOND) as wStart,count(*) from ubt GROUP BY SUBSTRING(data,3,5),TUMBLE(proctime, INTERVAL '1' SECOND)");

		tableEnv.toAppendStream(table, Row.class).print();
		env.execute();
	}

}
