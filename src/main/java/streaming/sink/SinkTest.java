package streaming.sink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.Test;
import streaming.dashboard.GeneData;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class SinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        DataStream<Tuple5<String, Integer, Long, Integer, Integer>> ds = env.addSource(new GeneData()).
                flatMap((String value, Collector<Tuple5<String, Integer, Long, Integer, Integer>> out) -> {

                    String ss[] = value.split(",");

                    out.collect(Tuple5.of(ss[0], Integer.parseInt(ss[1]), Long.parseLong(ss[2]), Integer.parseInt(ss[3]), Integer.parseInt(ss[4])));
                }).returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG, Types.INT, Types.INT))
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple5<String, Integer, Long, Integer, Integer>>() {
                    @Nullable
                    @Override
                    public Watermark checkAndGetNextWatermark(Tuple5<String, Integer, Long, Integer, Integer> lastElement, long extractedTimestamp) {
                        return new Watermark(lastElement.f2 - 3000);
                    }

                    @Override
                    public long extractTimestamp(Tuple5<String, Integer, Long, Integer, Integer> element, long previousElementTimestamp) {
                        return element.f2;
                    }
                });

        tenv.registerDataStream("log", ds, "traceid,userid,timestamp,status,restime,proctime.proctime,rowtime.rowtime");
        Table table = tenv.sqlQuery("select userid,count(*),TUMBLE_START(rowtime,INTERVAL '5' SECOND)  as starttime from log group  by userid,TUMBLE(rowtime,INTERVAL '5' SECOND)");

        tenv.toAppendStream(table, Row.class).print();
        env.execute();
    }

    @Test
    public void testRetractStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        List<Tuple2<String, String>> list = new ArrayList();
        list.add(Tuple2.of("mary", "home"));
        list.add(Tuple2.of("bob", "cart"));
        list.add(Tuple2.of("mary", "prod"));
        list.add(Tuple2.of("liz", "home"));
        list.add(Tuple2.of("bob", "home"));

        DataStream ds = env.fromCollection(list);
        tenv.registerDataStream("clicks", ds, "user,url,proctime.proctime");
        Table table = tenv.sqlQuery("select user,count(url) from clicks group by user");
        tenv.toRetractStream(table, Row.class).print();
        env.execute();

    }
}
