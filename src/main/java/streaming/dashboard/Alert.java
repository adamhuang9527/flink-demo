package streaming.dashboard;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 * 每秒钟用户请求的频次大于一定的阈值就报警
 */
public class Alert {

    public static void main(String[] args) throws Exception {
        // traceid,userid,timestamp,status,response time
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<Tuple5<String, Integer, Long, Integer, Integer>> ds = env.addSource(new GeneData()).
                flatMap((String value, Collector<Tuple5<String, Integer, Long, Integer, Integer>> out) -> {

                    String ss[] = value.split(",");

                    out.collect(Tuple5.of(ss[0], Integer.parseInt(ss[1]), Long.parseLong(ss[2]), Integer.parseInt(ss[3]), Integer.parseInt(ss[4])));
                }).returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG, Types.INT, Types.INT))
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple5<String, Integer, Long, Integer, Integer>>() {
                    @Nullable
                    @Override
                    public Watermark checkAndGetNextWatermark(Tuple5<String, Integer, Long, Integer, Integer> lastElement, long extractedTimestamp) {
                        return new Watermark(lastElement.f2 - 500);
                    }

                    @Override
                    public long extractTimestamp(Tuple5<String, Integer, Long, Integer, Integer> element, long previousElementTimestamp) {
                        return element.f2;
                    }
                });

        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
        tEnv.registerDataStream("logs", ds, "traceid,userid,timestamp,status,restime,proctime.proctime,rowtime.rowtime");
        String sql = "select TUMBLE_START(rowtime,INTERVAL '5' SECOND),userid,count(*) as pv from logs  " +
                "group by userid,TUMBLE(rowtime,INTERVAL '5' SECOND) having count(*) > 13500";
        Table result = tEnv.sqlQuery(sql);

        tEnv.toAppendStream(result, Row.class).print();

        env.execute();

    }
}
