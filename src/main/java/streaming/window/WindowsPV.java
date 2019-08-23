package streaming.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import table.SqlWindow;
import table.SqlWindow.Result;

import java.util.Properties;

/**
 * @author user
 */
public class WindowsPV {

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

        data.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(5))).aggregate(
                new AggregateFunction<Tuple4<String, String, Long, Integer>, PVAccumulator, SqlWindow.Result>() {

                    @Override
                    public PVAccumulator createAccumulator() {
                        // TODO Auto-generated method stub
                        return null;
                    }

                    @Override
                    public PVAccumulator add(Tuple4<String, String, Long, Integer> value, PVAccumulator accumulator) {
                        // TODO Auto-generated method stub
                        return null;
                    }

                    @Override
                    public Result getResult(PVAccumulator accumulator) {
                        // TODO Auto-generated method stub
                        return null;
                    }

                    @Override
                    public PVAccumulator merge(PVAccumulator a, PVAccumulator b) {
                        // TODO Auto-generated method stub
                        return null;
                    }
                }).print();

//		DataStream<Tuple2<String, Integer>> result = data
//				.map(new MapFunction<Tuple4<String, String, Long, Integer>, Tuple2<String, Integer>>() {
//
//					@Override
//					public Tuple2<String, Integer> map(Tuple4<String, String, Long, Integer> value) throws Exception {
//						return new Tuple2<String, Integer>(value.f0, value.f3);
//					}
//				});
//
//		result.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(5))).sum(1).print();

        env.execute();

    }

    public static class PVAccumulator {

    }

    private static class MyFlatMapFunction extends RichFlatMapFunction<String, Tuple4<String, String, Long, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple4<String, String, Long, Integer>> out) throws Exception {
            // TODO Auto-generated method stub

        }


    }
}
