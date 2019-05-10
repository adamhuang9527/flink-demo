package metrics;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.atomic.AtomicLong;

public class Monitor {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        DataStream<String> ds = env.addSource(new SourceFunction<String>() {

            private volatile boolean isRunning = true;
            AtomicLong al = new AtomicLong();

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (isRunning) {
                    Thread.sleep(10);
                    ctx.collect("value" + al.incrementAndGet());
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        ds.map(new MyMapper()).print();



        env.execute();
    }

    public static class MyMapper extends RichMapFunction<String, String> {
        private transient Counter counter;

        @Override
        public void open(Configuration config) {
            this.counter = getRuntimeContext()
                    .getMetricGroup()
                    .counter("myCounter");
        }

        @Override
        public String map(String value) throws Exception {
            this.counter.inc();
            return value;
        }
    }
}
