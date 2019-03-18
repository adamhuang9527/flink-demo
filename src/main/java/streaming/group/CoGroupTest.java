package streaming.group;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class CoGroupTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);


        DataStream<JSONObject> ds1 = env.addSource(new MySourceA());
        DataStream<JSONObject> ds2 = env.addSource(new MySourceB());


        ds1.join(ds2).where(m -> m.get("id")).equalTo(m -> m.get("id")).window(TumblingEventTimeWindows.of(Time.seconds(5))).apply((JSONObject first, JSONObject second) -> {

            System.out.println("first :" + first.get("id") + "   " + second.get("id"));
//                System.out.println("second :" + second);
            return first;

        }).print();

//        ds1.coGroup(ds2).where(m -> m.get("id")).equalTo(m -> m.get("id")).window(TumblingEventTimeWindows.of(Time.seconds(5))).apply(new CoGroupFunction<JSONObject, JSONObject, Object>() {
//            @Override
//            public void coGroup(Iterable<JSONObject> first, Iterable<JSONObject> second, Collector<Object> out) throws Exception {
//                JSONObject json = new JSONObject();
//                JSONArray ja = new JSONArray();
//                first.forEach(m -> {
//                    second.forEach(n -> {
//                        if(n.get("id").equals(m.get("id"))){
//                            ja.add(n.get("id"));
//                            System.out.println("first :" + m);
//                            System.out.println("second :" + n);
//                        }
//                    });
//                });
//
//                json.put("ids", ja);
//                out.collect(json);
//            }
//        }).print();
        env.execute();


//        DataStream<Integer> stream1 = env.fromElements(1, 2, 3);
//        DataStream<Integer> stream2 = env.fromElements(4, 2, 3);
//
////        stream1.join(stream2).where(0).equalTo(0).window(TumblingEventTimeWindows.of(Time.seconds(5)))
//        stream1.coGroup(stream2).where(m -> m).equalTo(m -> m)
//                .window(TumblingEventTimeWindows.of(Time.seconds(5)).apply();

    }


    private static class MySourceA implements SourceFunction<JSONObject> {
        private volatile boolean running = true;
        int status[] = {200, 404, 500, 501, 301};

        @Override
        public void run(SourceContext ctx) throws Exception {
            while (running) {
                Thread.sleep(500);
                JSONObject json = new JSONObject();
                json.put("id", (int) (Math.random() * 10));
                json.put("path", "A-B");
                json.put("responsetime", (int) (Math.random() * 100));
                json.put("status", status[(int) (Math.random() * 4)]);
                ctx.collect(json);
            }
        }

        @Override
        public void cancel() {
            running = false;

        }
    }


    private static class MySourceB implements SourceFunction<JSONObject> {
        private volatile boolean running = true;
        int status[] = {200, 404, 500, 501, 301};

        @Override
        public void run(SourceContext ctx) throws Exception {
            while (running) {
                Thread.sleep(500);
                JSONObject json = new JSONObject();
                json.put("id", (int) (Math.random() * 10));
                json.put("path", "B-C");
                json.put("responsetime", (int) (Math.random() * 100));
                json.put("status", status[(int) (Math.random() * 4)]);
                ctx.collect(json);
            }
        }

        @Override
        public void cancel() {
            running = false;

        }
    }

}
