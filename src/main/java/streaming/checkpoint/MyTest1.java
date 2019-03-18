package streaming.checkpoint;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import streaming.dashboard.GeneData;

import javax.annotation.Nullable;
import java.time.ZoneId;

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
        sink.setUseTruncate(false);
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
                return element.getResponseTime();
            }
        }).keyBy("userid").timeWindow(Time.seconds(5)).sum("responseTime").print();





        env.execute("my checkpoint test");
    }

    private static class UI {
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
