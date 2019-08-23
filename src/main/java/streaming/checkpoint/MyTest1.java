package streaming.checkpoint;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.util.Collector;
import streaming.dashboard.GeneData;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

public class MyTest1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setStateBackend(new RocksDBStateBackend("hdfs://localhost/checkpoints-data/", true));

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

////        FlinkKafkaConsumer011 f =  new FlinkKafkaConsumer011();
//        FlinkKafkaProducer011 p = new FlinkKafkaProducer011();
//
//        FlinkKafkaProducer011.Semantic.EXACTLY_ONCE


        DataStream<UI> ds = env.addSource(new GeneData()).map(new MapFunction<String, UI>() {
            @Override
            public UI map(String value) throws Exception {
                String ss[] = value.split(",");
                UI ui = new UI();
                ui.setTraceid(ss[0]);
                ui.setUserid(ss[1]);
                ui.setTimestamp(Long.parseLong(ss[2]));
                ui.setStatus(Integer.parseInt(ss[3]));
                ui.setResponseTime(Integer.parseInt(ss[4]));
                return ui;
            }
        });


        //write to hdfs sink
        BucketingSink<UI> sink = new BucketingSink<>("hdfs://localhost/logs/");
//        sink.setUseTruncate(false);
        sink.setBucketer(new DateTimeBucketer<UI>("yyyy-MM-dd--HH", ZoneId.of("UTC+8")));
        sink.setWriter(new StringWriter<UI>());
        sink.setBatchSize(1024 * 1024 * 10); // this is 10 MB,
        sink.setBatchRolloverInterval(60 * 1000); // this is 1 min


        ds.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<UI>() {
            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(UI lastElement, long extractedTimestamp) {
                return new Watermark(lastElement.getTimestamp() - 1000);
            }

            @Override
            public long extractTimestamp(UI element, long previousElementTimestamp) {
                return element.getTimestamp();
            }
        }).keyBy("userid").timeWindow(Time.seconds(10)).aggregate(new CountAgg(), new WindowResultFunction())
                .keyBy("windowEnd").process(new KeyedProcessFunction<Tuple, ItemViewCount, Object>() {

            private transient ListState<ItemViewCount> itemState;

            @Override
            public void processElement(ItemViewCount value, Context ctx, Collector<Object> out) throws Exception {
                // 每条数据都保存到状态中
                itemState.add(value);
                // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
                ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
                List<ItemViewCount> queue = new ArrayList<>();
                for (ItemViewCount item : itemState.get()) {
                    queue.add(item);
                }

                queue.sort((o1, o2) -> (int) (o2.viewCount - o1.viewCount));


                // 将排名信息格式化成 String, 便于打印
                StringBuilder result = new StringBuilder();
                result.append("====================================\n");
                result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n");

                for (int i = 0; i < 10; i++) {
                    ItemViewCount ivc = queue.get(i);
                    result.append(":").append("  商品ID=").append(ivc.itemId).append("  浏览量=")
                            .append(ivc.viewCount).append("\n");
                }


                result.append("====================================\n\n");

                out.collect(result.toString());

            }


            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ListStateDescriptor<ItemViewCount> itemsStateDesc = new ListStateDescriptor<>("itemState-state",
                        ItemViewCount.class);
                itemState = getRuntimeContext().getListState(itemsStateDesc);
            }
        }).print();


        env.execute("my checkpoint test");
    }


    /**
     * 用于输出窗口的结果
     */
    public static class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

        @Override
        public void apply(
                Tuple key,  // 窗口的主键，即 itemId
                TimeWindow window,  // 窗口
                Iterable<Long> aggregateResult, // 聚合函数的结果，即 count 值
                Collector<ItemViewCount> collector  // 输出类型为 ItemViewCount
        ) throws Exception {
            String itemId = ((Tuple1<String>) key).f0;
            Long count = aggregateResult.iterator().next();

//            for (long c : aggregateResult) {
//                System.out.println("c:   " + c);
//            }
            collector.collect(ItemViewCount.of(itemId, window.getEnd(), count));
        }
    }

    /**
     * 商品点击量(窗口操作的输出类型)
     */
    public static class ItemViewCount {
        public String itemId;     // 商品ID
        public long windowEnd;  // 窗口结束时间戳
        public long viewCount;  // 商品的点击量

        public static ItemViewCount of(String itemId, long windowEnd, long viewCount) {
            ItemViewCount result = new ItemViewCount();
            result.itemId = itemId;
            result.windowEnd = windowEnd;
            result.viewCount = viewCount;
            return result;
        }


        @Override
        public String toString() {
            return "ItemViewCount{" +
                    "itemId='" + itemId + '\'' +
                    ", windowEnd=" + windowEnd +
                    ", viewCount=" + viewCount +
                    '}';
        }
    }

    public static class CountAgg implements AggregateFunction<UI, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UI ui, Long acc) {
            return acc + ui.getResponseTime();
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long acc1, Long acc2) {
            return acc1 + acc2;
        }
    }

    public static class UI {
        private String traceid;
        private String userid;
        private long timestamp;
        private int status;
        private int responseTime;

        public int getResponseTime() {
            return responseTime;
        }

        public void setResponseTime(int responseTime) {
            this.responseTime = responseTime;
        }

        public String getTraceid() {
            return traceid;
        }

        public void setTraceid(String traceid) {
            this.traceid = traceid;
        }

        public String getUserid() {
            return userid;
        }

        public void setUserid(String userid) {
            this.userid = userid;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public int getStatus() {
            return status;
        }

        public void setStatus(int status) {
            this.status = status;
        }


        @Override
        public String toString() {
            return "UI{" +
                    "traceid='" + traceid + '\'' +
                    ", userid='" + userid + '\'' +
                    ", timestamp=" + timestamp +
                    ", status=" + status +
                    ", responseTime=" + responseTime +
                    '}';
        }
    }
}
